#![forbid(unsafe_code)]

//! Reconnect, fencing, and recovery contracts for the real `limitless::supervisor` and
//! `limitless::connection` code, driven against the scripted controlled peer.
//!
//! Every assertion reads the book through a [`BookObserver`] attached to the supervisor's
//! own writer, so what is proven here is what a consumer would see. Timing comes from the
//! peer's script and from the venue-negotiated heartbeat cadence; nothing is synchronized
//! by sleeping and hoping.

mod support;

use pm_ws::limitless::supervisor::{Supervisor, SupervisorConfig, SupervisorStats};
use pm_ws::{
    AuthorityReason, AuthorityState, BookObserver, ContinuityReason, Level, MutationContinuity,
    ObserverRecvError, PublishedBook, Side, StreamDelivery,
};
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const SLUG: &str = "btc-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const QUIET_PING_INTERVAL: Duration = Duration::from_millis(400);
const QUIET_PING_CYCLES: usize = 6;
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
const PATIENT_RESUBSCRIBE_WINDOW: Duration = Duration::from_secs(30);
const SWALLOWED_RESUBSCRIBE_WINDOW: Duration = Duration::from_millis(300);
/// [`PeerConfig::default`]'s announced ping interval plus its ping timeout, which is the
/// deadline the daemon derives from them.
const DEFAULT_HEARTBEAT_DEADLINE: Duration = Duration::from_millis(2_000);
const STREAM_BURST: u64 = 3;
const STREAM_BURST_INTERVAL: Duration = Duration::from_millis(12);
const STREAM_OVERSHOOT: Duration = Duration::from_millis(400);
const STREAM_OBSERVER_CAPACITY: usize = 4096;
/// A burst overflows the capacity-one ingest queue; the pause that follows lets the
/// supervisor drain a slot while the connection is parked on its socket with the loss still
/// unreported, which is the moment the next burst's first frame must not be allowed to take.
/// The smallest ingest queue that can carry a connection's establishment notices — the
/// venue-negotiated open, then the venue's acknowledgment of the subscription — before the
/// first book frame arrives. A shorter queue drops early frames while those markers wait for
/// room, which is ordinary bounded-queue behavior and not what an overload test is about.
const ESTABLISHMENT_INGEST_FLOOR: usize = 2;
/// Long enough for the connection's pending loss report to reach the supervisor and for the
/// queue to empty behind it, so a recovery frame is offered into room rather than dropped.
const OVERLOAD_RECOVERY_INTERVAL: Duration = Duration::from_millis(150);
/// The ceiling of the escalating quiet window held after each recovery frame: a frame sent
/// into a still-draining queue is dropped like any other, and a book made live by one is
/// re-staled by the next, so recovery is only observable once a frame meets an empty queue
/// and nothing follows it while the observer looks. The window doubles per attempt up to
/// this cap, which bounds how long a slow host's backlog may take to drain between offers.
const OVERLOAD_RECOVERY_SETTLE_CAP: Duration = Duration::from_secs(2);
const OVERLOAD_BURST: u64 = 10;
const OVERLOAD_ROUNDS: u64 = 20;
const OVERLOAD_ROUND_INTERVAL: Duration = Duration::from_millis(2);
/// Long enough that the first base is committed well before the diagnostic kill fires,
/// short enough to keep the test in seconds.
const KILL_PRIMARY_AFTER: Duration = Duration::from_secs(1);

/// A peer whose announced heartbeat cadence is far longer than any test step, so that a
/// test about something other than liveness cannot be disturbed by a heartbeat deadline.
/// [`PeerConfig::default`] announces one second, the smallest cadence this daemon accepts
/// from a decoded open packet, which is what a heartbeat test wants.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// Production policy with the reconnect clock compressed: the same code paths, spaced so a
/// test finishes in seconds. `fenced_linger` is deliberately long so a fenced generation is
/// still draining when a test hands it a late frame, and `resubscribe_window` is deliberately
/// far longer than any step so that only the test that scripts a swallowed resubscription
/// ever sees the window expire.
fn test_config(endpoint: String) -> SupervisorConfig {
    SupervisorConfig {
        endpoint,
        market: SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        max_recovery_attempts: 2,
        fenced_linger: Duration::from_secs(10),
        resubscribe_window: PATIENT_RESUBSCRIBE_WINDOW,
        ..SupervisorConfig::default()
    }
}

struct Running {
    observer: BookObserver,
    stop: pm_ws::limitless::supervisor::Stopper,
    handle: JoinHandle<SupervisorStats>,
}

impl Running {
    fn start(endpoint: String) -> Self {
        Self::start_with(test_config(endpoint))
    }

    fn start_with(config: SupervisorConfig) -> Self {
        let mut supervisor =
            Supervisor::new(config).expect("test supervisor configuration is valid");
        let observer = supervisor.attach();
        let stop = supervisor.stopper();
        let handle =
            tokio::spawn(async move { supervisor.run_until(Instant::now() + RUN_CAP).await });
        Self {
            observer,
            stop,
            handle,
        }
    }

    async fn finish(self) -> SupervisorStats {
        self.stop.stop();
        tokio::time::timeout(STEP_TIMEOUT, self.handle)
            .await
            .expect("supervisor run ends after stop")
            .expect("supervisor task completes")
    }
}

/// Waits until the published book satisfies `predicate`, or panics naming what it was
/// waiting for and what it last saw.
async fn await_book(
    observer: &mut BookObserver,
    what: &str,
    predicate: impl Fn(&PublishedBook) -> bool,
) -> Arc<PublishedBook> {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let latest = observer.latest();
        if predicate(&latest) {
            return latest;
        }
        match tokio::time::timeout_at(deadline, observer.state_changed()).await {
            Err(_) => panic!(
                "timed out waiting for {what}; last seen revision={} authority={:?} epoch={}",
                latest.revision(),
                latest.authority(),
                latest.continuity().epoch()
            ),
            Ok(Err(_)) => panic!("the book writer went away while waiting for {what}"),
            Ok(Ok(_)) => {}
        }
    }
}

fn is_live(published: &PublishedBook) -> bool {
    published.authority() == &AuthorityState::Live
}

fn is_stale(published: &PublishedBook, reason: AuthorityReason) -> bool {
    published.authority() == &AuthorityState::Stale(reason)
}

fn levels_of(published: &PublishedBook, side: Side) -> Vec<(String, String)> {
    published
        .canonical_levels()
        .iter()
        .filter(|level| level.side() == side)
        .map(|level| {
            (
                level.price().value().canonical(),
                level.quantity().value().canonical(),
            )
        })
        .collect()
}

fn expect_levels(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(price, size)| ((*price).to_owned(), (*size).to_owned()))
        .collect()
}

/// Sends one snapshot whose bid quantity and `version` are both `version`, so the peer's
/// stream is numbered and any snapshot the daemon skipped is visible in what it commits.
async fn send_numbered(connection: &mut PeerConnection, version: u64) {
    let size = version.to_string();
    connection
        .send_orderbook(
            SLUG,
            &[("0.5", size.as_str())],
            &[("0.6", "200")],
            Some(version),
        )
        .await;
}

fn quantity_of(level: &Level) -> u64 {
    level
        .quantity()
        .value()
        .canonical()
        .parse()
        .expect("the peer's numbered quantities are whole numbers")
}

/// Drains the mutation surface, asserting every derived mutation moves the bid quantity by
/// exactly one step, and returns how many it checked.
///
/// Against [`send_numbered`]'s stream this is the statement that no snapshot was ever
/// committed as the continuation of one that never reached the book: a snapshot that was
/// dropped, fenced, or otherwise skipped leaves the next accepted one either deriving a
/// jump — which fails here — or opening a new epoch as a recovery base, which derives
/// nothing at all and is the only truthful way to cross a gap.
///
/// Crossing into that new epoch is not something an attachment may do silently: the state
/// replacement the epoch opened with travelled as state, never as mutations, so a drain that
/// kept applying would skip it. The drain is told instead, and restarts from the rebased
/// state. Every other break still fails it.
fn assert_bid_mutations_step_by_one(observer: &mut BookObserver) -> usize {
    let mut steps = 0;
    loop {
        match observer.try_recv() {
            Ok(None) => return steps,
            Ok(Some(StreamDelivery::Resolution(delivery))) => {
                panic!("this book publishes no resolution: {delivery:?}")
            }
            Ok(Some(StreamDelivery::Mutation(delivery))) => {
                let mutation = delivery.mutation();
                let (Some(old), Some(new)) = (mutation.old(), mutation.replacement()) else {
                    continue;
                };
                let (old, new) = (quantity_of(old), quantity_of(new));
                assert_eq!(
                    new,
                    old + 1,
                    "revision {} continued the book across a snapshot that never reached it: \
                     {old} to {new}",
                    delivery.revision()
                );
                steps += 1;
            }
            Err(ObserverRecvError::ContinuityLost {
                reason: ContinuityReason::RecoveryBase,
                missed,
            }) => {
                assert_eq!(
                    missed, 0,
                    "a rebase drops no delivery of its own; the loss is the rebase"
                );
                let _ = observer.reattach();
            }
            Err(error) => panic!("the mutation surface broke: {error:?}"),
        }
    }
}

/// Asserts a recorded revision sequence never repeats and never rolls back.
fn assert_strictly_increasing(revisions: &[u64]) {
    for pair in revisions.windows(2) {
        assert!(
            pair[1] > pair[0],
            "book revision rolled back or repeated: {revisions:?}"
        );
    }
}

#[tokio::test]
async fn abrupt_drop_resubscribes_and_commits_a_recovery_base() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let mut revisions = Vec::new();

    let mut first = peer.next_connection().await;
    assert_eq!(
        first.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );
    first
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    revisions.push(base.revision());
    assert_eq!(base.continuity().epoch(), 0);

    first
        .send_orderbook(SLUG, &[("0.5", "150")], &[("0.6", "200")], Some(2))
        .await;
    let changed = await_book(&mut running.observer, "the second snapshot", |published| {
        is_live(published) && published.revision() > base.revision()
    })
    .await;
    revisions.push(changed.revision());
    let delivery = tokio::time::timeout(STEP_TIMEOUT, running.observer.recv())
        .await
        .expect("a derived mutation arrives for the second snapshot")
        .expect("the mutation surface stays continuous");
    assert_eq!(delivery.cursor().epoch(), 0);
    while let Ok(Some(_)) = running.observer.try_recv() {}

    first.drop_abruptly().await;
    let stale = await_book(&mut running.observer, "Stale(Disconnect)", |published| {
        is_stale(published, AuthorityReason::Disconnect)
    })
    .await;
    revisions.push(stale.revision());
    assert!(matches!(
        stale.continuity(),
        MutationContinuity::Lost {
            epoch: 0,
            reason: pm_ws::ContinuityReason::Reconnect
        }
    ));

    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "the new generation resubscribes the full desired set"
    );
    second
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(3))
        .await;
    let recovered = await_book(&mut running.observer, "the recovery base", is_live).await;
    revisions.push(recovered.revision());
    assert_eq!(
        recovered.continuity().epoch(),
        1,
        "a recovery base opens exactly one new continuity epoch"
    );
    assert!(
        matches!(running.observer.try_recv(), Ok(None)),
        "a recovery base derives no mutation across the gap"
    );
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.4", "300")])
    );
    assert_eq!(
        levels_of(&recovered, Side::Ask),
        expect_levels(&[("0.7", "50")])
    );
    assert_strictly_increasing(&revisions);

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.snapshots_applied, 3);
    assert_eq!(stats.continuity_losses, 1);
}

#[tokio::test]
async fn heartbeat_timeout_fences_a_late_frame_from_the_closed_generation() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let mut running = Running::start(peer.endpoint());

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    first
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;

    let stale = await_book(
        &mut running.observer,
        "Stale(Disconnect) from the missed heartbeat deadline",
        |published| is_stale(published, AuthorityReason::Disconnect),
    )
    .await;
    assert!(stale.revision() > base.revision());

    let mut second = peer.next_connection().await;
    second.complete_handshake().await;

    first
        .send_orderbook(SLUG, &[("0.9", "999")], &[("0.95", "999")], Some(99))
        .await;
    first.send_ping().await;
    first.expect_pong(STEP_TIMEOUT).await;

    second
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(2))
        .await;
    let recovered = await_book(&mut running.observer, "the recovery base", is_live).await;
    assert_eq!(recovered.continuity().epoch(), 1);
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.4", "300")])
    );
    assert_eq!(
        levels_of(&recovered, Side::Ask),
        expect_levels(&[("0.7", "50")])
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.fenced_events, 1,
        "the late frame reached the supervisor and was discarded by generation"
    );
    assert_eq!(stats.fenced_generations, 1);
    assert_eq!(
        stats.snapshots_applied, 2,
        "only the two live-generation snapshots ever committed"
    );
    assert_eq!(stats.connection_attempts, 2);
}

/// Pins the `--kill-primary-after` flag's plumbing: armed, it kills the publishing socket
/// for real, and what follows is the ordinary end-of-connection machinery pinned elsewhere
/// in this file rather than anything the flag reports itself.
#[tokio::test]
async fn the_diagnostic_kill_flag_kills_the_publishing_socket_and_recovery_proceeds() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start_with(SupervisorConfig {
        kill_primary_after: Some(KILL_PRIMARY_AFTER),
        ..test_config(peer.endpoint())
    });

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    first
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    assert_eq!(
        base.continuity().epoch(),
        0,
        "the base must commit before the kill fires, or nothing after it is about the kill"
    );

    assert!(
        first.read_text_frame(STEP_TIMEOUT).await.is_none(),
        "the peer's socket dies when the armed kill fires"
    );
    await_book(
        &mut running.observer,
        "Stale(LocalLoss) from the killed connection",
        |published| is_stale(published, AuthorityReason::LocalLoss),
    )
    .await;

    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "the replacement generation resubscribes the full desired set"
    );
    second
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(2))
        .await;
    let recovered = await_book(&mut running.observer, "the recovery base", is_live).await;
    assert_eq!(
        recovered.continuity().epoch(),
        1,
        "the killed connection cost exactly one continuity epoch"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.connection_attempts, 2,
        "the kill arms once, so the replacement generation is not killed too"
    );
    assert_eq!(stats.continuity_losses, 1);
}

#[tokio::test]
async fn quiet_market_with_a_healthy_heartbeat_stays_live() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let mut running = Running::start(peer.endpoint());

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    connection
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;

    for _ in 0..QUIET_PING_CYCLES {
        tokio::time::sleep(QUIET_PING_INTERVAL).await;
        connection.send_ping().await;
        connection.expect_pong(STEP_TIMEOUT).await;
        let latest = running.observer.latest();
        assert!(
            is_live(&latest),
            "a quiet market with a healthy heartbeat stayed {:?}",
            latest.authority()
        );
        assert_eq!(latest.revision(), base.revision());
    }

    let stats = running.finish().await;
    assert_eq!(
        stats.connection_attempts, 1,
        "silence must not trigger a reconnect"
    );
    assert_eq!(stats.continuity_losses, 0);
    assert_eq!(stats.fenced_generations, 0);
}

#[tokio::test]
async fn exhausted_recovery_reports_recovery_base_unavailable_then_recovers() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    first
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    await_book(&mut running.observer, "the first accepted base", is_live).await;
    first.drop_abruptly().await;
    await_book(&mut running.observer, "Stale(Disconnect)", |published| {
        is_stale(published, AuthorityReason::Disconnect)
    })
    .await;

    let mut second = peer.next_connection().await;
    second.complete_handshake().await;
    second.drop_abruptly().await;

    let mut third = peer.next_connection().await;
    assert!(
        is_stale(&running.observer.latest(), AuthorityReason::Disconnect),
        "one spent attempt is not exhaustion: {:?}",
        running.observer.latest().authority()
    );
    third.complete_handshake().await;
    third.drop_abruptly().await;

    await_book(
        &mut running.observer,
        "Stale(RecoveryBaseUnavailable)",
        |published| is_stale(published, AuthorityReason::RecoveryBaseUnavailable),
    )
    .await;

    let mut fourth = peer.next_connection().await;
    assert_eq!(
        fourth.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "retries continue after exhaustion"
    );
    fourth.drop_abruptly().await;

    let mut fifth = peer.next_connection().await;
    let after_a_later_failure = running.observer.latest();
    assert!(
        is_stale(
            &after_a_later_failure,
            AuthorityReason::RecoveryBaseUnavailable
        ),
        "a retry that failed after exhaustion overwrote the standing terminal reason with \
         its own: {:?}",
        after_a_later_failure.authority()
    );
    assert_eq!(
        fifth.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );
    fifth
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(2))
        .await;
    let recovered = await_book(&mut running.observer, "the late recovery base", is_live).await;
    assert_eq!(recovered.continuity().epoch(), 1);

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 5);
    assert_eq!(
        stats.recovery_base_unavailable, 1,
        "the notification is one-time even though the condition stood across two failures"
    );
    assert_eq!(stats.snapshots_applied, 2);
}

#[tokio::test]
async fn a_frame_read_at_the_heartbeat_deadline_never_continues_the_book() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let mut running = Running::start_with(SupervisorConfig {
        observer_capacity: STREAM_OBSERVER_CAPACITY,
        ..test_config(peer.endpoint())
    });

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    let until = Instant::now() + DEFAULT_HEARTBEAT_DEADLINE + STREAM_OVERSHOOT;
    let mut version: u64 = 1;
    while Instant::now() < until {
        for _ in 0..STREAM_BURST {
            send_numbered(&mut connection, version).await;
            version += 1;
        }
        tokio::time::sleep(STREAM_BURST_INTERVAL).await;
    }

    let stale = await_book(
        &mut running.observer,
        "Stale(Disconnect) from the missed heartbeat deadline",
        |published| is_stale(published, AuthorityReason::Disconnect),
    )
    .await;
    assert!(stale.revision() > 1);
    let steps = assert_bid_mutations_step_by_one(&mut running.observer);
    assert!(
        steps > 0,
        "the stream must have committed something for this window to prove anything"
    );

    let stats = running.finish().await;
    assert_eq!(stats.fenced_generations, 1);
    assert!(
        stats.fenced_events > 0,
        "frames were still arriving from the closed generation, which is the window under test"
    );
}

#[tokio::test]
async fn an_overloaded_ingest_queue_reports_the_loss_before_what_followed_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start_with(SupervisorConfig {
        ingest_capacity: ESTABLISHMENT_INGEST_FLOOR,
        observer_capacity: STREAM_OBSERVER_CAPACITY,
        ..test_config(peer.endpoint())
    });

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_numbered(&mut connection, 1).await;
    await_book(&mut running.observer, "the first accepted base", is_live).await;

    let mut version: u64 = 2;
    for _ in 0..OVERLOAD_ROUNDS {
        for _ in 0..OVERLOAD_BURST {
            send_numbered(&mut connection, version).await;
            version += 1;
        }
        tokio::time::sleep(OVERLOAD_ROUND_INTERVAL).await;
    }
    let broken = await_book(
        &mut running.observer,
        "Stale(Overload) from the overflowing ingest queue",
        |published| is_stale(published, AuthorityReason::Overload),
    )
    .await;
    assert!(broken.revision() > 1);

    let recovery_deadline = Instant::now() + STEP_TIMEOUT;
    let mut settle = OVERLOAD_RECOVERY_INTERVAL;
    let recovered = 'recovery: loop {
        let latest = running.observer.latest();
        if is_live(&latest) {
            break latest;
        }
        assert!(
            Instant::now() < recovery_deadline,
            "timed out waiting for a base accepted after the loss was reported; \
             last seen revision={} authority={:?} epoch={}",
            latest.revision(),
            latest.authority(),
            latest.continuity().epoch()
        );
        send_numbered(&mut connection, version).await;
        version += 1;
        let settle_until = Instant::now() + settle;
        settle = (settle * 2).min(OVERLOAD_RECOVERY_SETTLE_CAP);
        loop {
            let latest = running.observer.latest();
            if is_live(&latest) {
                break 'recovery latest;
            }
            match tokio::time::timeout_at(settle_until, running.observer.state_changed()).await {
                Err(_) => break,
                Ok(changed) => {
                    changed.expect("the book writer stays alive while recovery is awaited");
                }
            }
        }
    };
    assert!(
        recovered.continuity().epoch() > 0,
        "what followed the loss was accepted as a fresh base, not as a continuation"
    );
    assert_bid_mutations_step_by_one(&mut running.observer);

    let stats = running.finish().await;
    assert!(
        stats.overload_drops > 0,
        "the capacity-one ingest queue must actually have overflowed"
    );
    assert!(stats.continuity_losses > 0);
    assert!(
        stats.decode_failures.is_empty(),
        "nothing here is malformed: the only loss is the queue's"
    );
    assert_eq!(
        stats.connection_attempts, 1,
        "an overloaded queue is recovered on the connection that overflowed it"
    );
}

#[tokio::test]
async fn malformed_event_reports_local_loss_without_reconnecting() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    connection
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;

    connection
        .send_orderbook(SLUG, &[("2", "100")], &[("0.6", "200")], Some(2))
        .await;
    let stale = await_book(&mut running.observer, "Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    assert!(stale.revision() > base.revision());
    assert_eq!(
        levels_of(&stale, Side::Bid),
        expect_levels(&[("0.5", "100")]),
        "a rejected candidate leaves the last valid state in place"
    );

    assert_eq!(
        connection.expect_resubscription(STEP_TIMEOUT).await.slugs,
        vec![SLUG.to_owned()],
        "a local loss asks the healthy connection for a fresh base instead of waiting for one"
    );

    connection
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(3))
        .await;
    let recovered = await_book(
        &mut running.observer,
        "the recovery base on the same connection",
        is_live,
    )
    .await;
    assert_eq!(recovered.continuity().epoch(), 1);
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.4", "300")])
    );
    assert_eq!(
        recovered
            .provenance()
            .expect("a committed revision carries provenance")
            .subscription_generation(),
        2,
        "the recovery base is stamped with the re-emit it arrived under, not the first one"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.connection_attempts, 1,
        "a purely local loss never replaces the connection"
    );
    assert_eq!(
        stats.decode_failures.get("event:PriceOutOfDomain"),
        Some(&1)
    );
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(stats.resubscribes_emitted, 1);
    assert_eq!(
        stats.resubscribes_escalated, 0,
        "the resubscription produced its base, so nothing escalated"
    );
}

#[tokio::test]
async fn a_swallowed_resubscribe_escalates_to_a_reconnect() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start_with(SupervisorConfig {
        resubscribe_window: SWALLOWED_RESUBSCRIBE_WINDOW,
        ..test_config(peer.endpoint())
    });

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    first
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;

    first
        .send_orderbook(SLUG, &[("2", "100")], &[("0.6", "200")], Some(2))
        .await;
    await_book(&mut running.observer, "Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;

    assert_eq!(
        first.expect_resubscription(STEP_TIMEOUT).await.slugs,
        vec![SLUG.to_owned()],
        "the resubscription is attempted before any reconnect"
    );

    let escalated = await_book(
        &mut running.observer,
        "Stale(SubscriptionLost) from the expired resubscribe window",
        |published| is_stale(published, AuthorityReason::SubscriptionLost),
    )
    .await;
    assert!(escalated.revision() > base.revision());

    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "the replacement generation carries its own subscription"
    );
    second
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(3))
        .await;
    let recovered = await_book(
        &mut running.observer,
        "the recovery base served by the replacement connection",
        is_live,
    )
    .await;
    assert_eq!(recovered.continuity().epoch(), 1);
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.4", "300")])
    );
    assert_strictly_increasing(&[base.revision(), escalated.revision(), recovered.revision()]);

    let stats = running.finish().await;
    assert_eq!(
        stats.resubscribes_escalated, 1,
        "the swallowed resubscription escalated exactly once"
    );
    assert!(
        stats.resubscribes_emitted >= 1,
        "the escalation followed an emitted resubscription"
    );
    assert_eq!(
        stats.connection_attempts, 2,
        "the resubscription was attempted before, not instead of, replacing the connection"
    );
    assert_eq!(
        stats.fenced_generations, 1,
        "the escalated generation was fenced before it was replaced"
    );
}
