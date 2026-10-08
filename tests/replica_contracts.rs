#![forbid(unsafe_code)]

//! Redundancy contracts for the real `limitless::supervisor` running `replicas = 2`,
//! driven against the scripted controlled peer.
//!
//! One peer serves both connections; each accepted connection is scripted separately, so a
//! test decides exactly what the primary and the standby each see. Which connection the
//! daemon assigned to which role is read from the diagnostics tap by Engine.IO session id
//! rather than assumed from accept ordering, so nothing here depends on a race. Every book
//! assertion reads through a [`BookObserver`] attached to the supervisor's own writer.

mod support;

use pm_ws::limitless::supervisor::{
    Stopper, Supervisor, SupervisorConfig, SupervisorNotice, SupervisorStats,
};
use pm_ws::{
    AuthorityReason, AuthorityState, BookObserver, DivergenceReason, MutationContinuity,
    PublishedBook, ReplicaRole, Side, SourceState, StandbyState,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The configured command floor, less the scheduling slack between the instant a command is
/// authorized and the instant its bytes reach the peer.
const PACING_FLOOR: Duration = Duration::from_millis(450);
const SLUG: &str = "btc-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const TAP_CAPACITY: usize = 1024;
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
const PATIENT_RESUBSCRIBE_WINDOW: Duration = Duration::from_secs(30);

/// A peer whose announced heartbeat cadence is far longer than any test step, so a test
/// about redundancy cannot be disturbed by a heartbeat deadline.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// Production policy with the reconnect clock compressed and one hot standby requested.
fn test_config(endpoint: String) -> SupervisorConfig {
    SupervisorConfig {
        endpoint,
        market: SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        replicas: 2,
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
    tap: mpsc::Receiver<SupervisorNotice>,
    stop: Stopper,
    handle: JoinHandle<SupervisorStats>,
}

impl Running {
    fn start(endpoint: String) -> Self {
        let (tap_tx, tap) = mpsc::channel(TAP_CAPACITY);
        let mut supervisor = Supervisor::new(test_config(endpoint))
            .expect("test supervisor configuration is valid")
            .with_diagnostics(tap_tx);
        let observer = supervisor.attach();
        let stop = supervisor.stopper();
        let handle =
            tokio::spawn(async move { supervisor.run_until(Instant::now() + RUN_CAP).await });
        Self {
            observer,
            tap,
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

/// Consumes diagnostics until one notice yields a value, or panics naming what it was
/// waiting for. Notices that do not match are discarded, so tests await in causal order.
async fn await_notice<T>(
    tap: &mut mpsc::Receiver<SupervisorNotice>,
    what: &str,
    mut select: impl FnMut(&SupervisorNotice) -> Option<T>,
) -> T {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, tap.recv()).await {
            Err(_) => panic!("timed out waiting for {what} on the diagnostics tap"),
            Ok(None) => panic!("the diagnostics tap closed while waiting for {what}"),
            Ok(Some(notice)) => {
                if let Some(value) = select(&notice) {
                    return value;
                }
            }
        }
    }
}

/// Consumes diagnostics like [`await_notice`], additionally recording every source
/// transition it passes over into `history`.
///
/// [`await_notice`] discards what it does not match, which would leave a later drain of the
/// tap blind to the transitions that happened while it was waiting. A test that asserts
/// something about every transition in a window records the window instead.
async fn await_notice_recording<T>(
    tap: &mut mpsc::Receiver<SupervisorNotice>,
    history: &mut Vec<SourceState>,
    what: &str,
    mut select: impl FnMut(&SupervisorNotice) -> Option<T>,
) -> T {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, tap.recv()).await {
            Err(_) => panic!("timed out waiting for {what} on the diagnostics tap"),
            Ok(None) => panic!("the diagnostics tap closed while waiting for {what}"),
            Ok(Some(notice)) => {
                if let Some(source) = source_of(&notice) {
                    history.push(source.clone());
                }
                if let Some(value) = select(&notice) {
                    return value;
                }
            }
        }
    }
}

/// Takes every source transition the tap currently holds, without waiting.
fn drain_transitions(tap: &mut mpsc::Receiver<SupervisorNotice>, history: &mut Vec<SourceState>) {
    while let Ok(notice) = tap.try_recv() {
        if let Some(source) = source_of(&notice) {
            history.push(source.clone());
        }
    }
}

/// The generation currently publishing, when the topology is publishing at all.
fn publishing(source: &SourceState) -> Option<u64> {
    source
        .publishing_primary()
        .map(|primary| primary.connection().generation())
}

/// The single assigned standby's generation and comparison verdict.
fn standby(source: &SourceState) -> Option<(u64, StandbyState)> {
    source
        .standbys()
        .map(|(standby, state)| (standby.connection().generation(), state.clone()))
        .next()
}

/// The generation being driven to obtain a fresh authoritative base.
fn recovering(source: &SourceState) -> Option<u64> {
    source
        .recovery()
        .map(|recovery| recovery.connection().generation())
}

fn source_of(notice: &SupervisorNotice) -> Option<&SourceState> {
    match notice {
        SupervisorNotice::SourceTransition { source } => Some(source),
        _ => None,
    }
}

/// Accepts and completes both of the supervisor's connections, then returns them ordered
/// (primary, standby) by the role the daemon reported for each Engine.IO session id.
async fn connect_pair(
    peer: &mut ControlledPeer,
    tap: &mut mpsc::Receiver<SupervisorNotice>,
) -> (PeerConnection, PeerConnection) {
    let mut first = peer.next_connection().await;
    assert_eq!(
        first.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );
    let first_subscribed = Instant::now();
    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "both replicas subscribe the same desired market set"
    );
    let gap = Instant::now().saturating_duration_since(first_subscribed);
    assert!(
        gap >= PACING_FLOOR,
        "one endpoint's command floor paces both roles' subscriptions at the wire; \
         the second landed {gap:?} after the first"
    );
    let mut roles: BTreeMap<String, ReplicaRole> = BTreeMap::new();
    while roles.len() < 2 {
        let (sid, replica) =
            await_notice(tap, "a connection announcement", |notice| match notice {
                SupervisorNotice::Connected { sid, replica, .. } => {
                    Some((sid.clone(), replica.clone()))
                }
                _ => None,
            })
            .await;
        let _ = roles.insert(sid, replica);
    }
    let role_of = |connection: &PeerConnection| {
        roles
            .get(&connection.engine_sid())
            .cloned()
            .expect("the daemon announced a role for every accepted connection")
    };
    match (role_of(&first), role_of(&second)) {
        (ReplicaRole::PublishingPrimary, ReplicaRole::HotStandby) => (first, second),
        (ReplicaRole::HotStandby, ReplicaRole::PublishingPrimary) => (second, first),
        (left, right) => panic!("expected one primary and one standby, got {left:?} and {right:?}"),
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

/// Asserts a recorded revision sequence never repeats and never rolls back.
fn assert_strictly_increasing(revisions: &[u64]) {
    for pair in revisions.windows(2) {
        assert!(
            pair[1] > pair[0],
            "book revision rolled back or repeated: {revisions:?}"
        );
    }
}

/// Waits until the tap reports the standby in `expected`, and returns its generation.
async fn await_standby(
    tap: &mut mpsc::Receiver<SupervisorNotice>,
    what: &str,
    expected: StandbyState,
) -> u64 {
    await_notice(tap, what, |notice| {
        source_of(notice)
            .and_then(standby)
            .filter(|(_, state)| state == &expected)
            .map(|(generation, _)| generation)
    })
    .await
}

#[tokio::test]
async fn an_agreeing_standby_takes_over_without_touching_the_book() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby agreeing with the published book",
        StandbyState::Agreeing,
    )
    .await;

    primary.drop_abruptly().await;

    let promoted = await_notice(
        &mut running.tap,
        "the standby taking over the publishing role",
        |notice| {
            source_of(notice)
                .and_then(publishing)
                .filter(|generation| *generation == standby_generation)
        },
    )
    .await;

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "standby coverage is rebuilt with the same desired market set"
    );
    let replacement_role = await_notice(
        &mut running.tap,
        "the replacement connection's announced role",
        |notice| match notice {
            SupervisorNotice::Connected { sid, replica, .. }
                if sid == &replacement.engine_sid() =>
            {
                Some(replica.clone())
            }
            _ => None,
        },
    )
    .await;
    assert_eq!(
        replacement_role,
        ReplicaRole::HotStandby,
        "a replacement connection joins as a standby, never as a second publisher"
    );

    standby_peer
        .send_orderbook(SLUG, &[("0.5", "150")], &[("0.6", "200")], Some(2))
        .await;
    let after = await_book(
        &mut running.observer,
        "a snapshot committed by the promoted source",
        |published| published.revision() > base.revision(),
    )
    .await;
    assert!(
        is_live(&after),
        "the book was never stale across the switch"
    );
    assert_eq!(
        after.revision(),
        base.revision() + 1,
        "a staleness report would have spent a revision between the base and this snapshot"
    );
    assert!(
        matches!(
            after.continuity(),
            MutationContinuity::Intact { epoch: 0, .. }
        ),
        "a seamless source switch opens no new epoch: {:?}",
        after.continuity()
    );
    let provenance = after
        .provenance()
        .expect("a committed revision carries provenance");
    assert_eq!(
        provenance.connection().generation(),
        promoted,
        "the promoted connection is the one that committed"
    );
    assert_eq!(provenance.replica(), &ReplicaRole::PublishingPrimary);
    let delivery = tokio::time::timeout(STEP_TIMEOUT, running.observer.recv())
        .await
        .expect("a derived mutation arrives for the post-failover snapshot")
        .expect("the mutation surface stayed continuous across the switch");
    assert_eq!(delivery.cursor().epoch(), 0);
    assert_strictly_increasing(&[base.revision(), after.revision()]);

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 1);
    assert_eq!(stats.promotions_refused, 0);
    assert_eq!(
        stats.continuity_losses, 0,
        "no continuity loss was ever reported to the book"
    );
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(stats.shadow_snapshots_applied, 1);
}

#[tokio::test]
async fn a_divergent_standby_is_refused_and_the_book_recovers_from_a_fresh_base() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;
    let mut revisions = Vec::new();

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.4", "999")], &[("0.7", "111")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    revisions.push(base.revision());
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby diverging on content",
        StandbyState::Divergent(DivergenceReason::ContentMismatch),
    )
    .await;

    primary.drop_abruptly().await;

    let stale = await_book(
        &mut running.observer,
        "Stale(ReplicaDivergence)",
        |published| is_stale(published, AuthorityReason::ReplicaDivergence),
    )
    .await;
    revisions.push(stale.revision());
    assert_eq!(
        levels_of(&stale, Side::Bid),
        expect_levels(&[("0.5", "100")]),
        "the refused shadow content was never published"
    );
    let recovery_generation = await_notice(
        &mut running.tap,
        "the surviving connection driving recovery",
        |notice| source_of(notice).and_then(recovering),
    )
    .await;
    assert_eq!(
        recovery_generation, standby_generation,
        "recovery runs on the surviving healthy connection"
    );

    assert_eq!(
        standby_peer.expect_resubscription(STEP_TIMEOUT).await.slugs,
        vec![SLUG.to_owned()],
        "the surviving connection is asked for a fresh base rather than waited on"
    );

    standby_peer
        .send_orderbook(SLUG, &[("0.45", "77")], &[("0.55", "88")], Some(2))
        .await;
    let recovered = await_book(
        &mut running.observer,
        "the fresh venue base installed on the surviving connection",
        is_live,
    )
    .await;
    revisions.push(recovered.revision());
    assert_eq!(
        recovered.continuity().epoch(),
        1,
        "a recovery base opens exactly one new continuity epoch"
    );
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.45", "77")]),
        "the book holds the fresh venue base, not the refused shadow"
    );
    assert_eq!(
        levels_of(&recovered, Side::Ask),
        expect_levels(&[("0.55", "88")])
    );
    assert_strictly_increasing(&revisions);

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 0);
    assert_eq!(stats.promotions_refused, 1);
    assert_eq!(stats.continuity_losses, 1);
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(
        stats.resubscribes_emitted, 1,
        "one refused promotion asks the surviving connection exactly once"
    );
    assert_eq!(stats.resubscribes_escalated, 0);
}

#[tokio::test]
async fn a_standby_without_a_base_is_refused_with_the_connection_reason() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby holding no comparable history",
        StandbyState::Divergent(DivergenceReason::ContinuityMismatch),
    )
    .await;

    primary.drop_abruptly().await;

    let stale = await_book(&mut running.observer, "Stale(Disconnect)", |published| {
        is_stale(published, AuthorityReason::Disconnect)
    })
    .await;
    assert!(stale.revision() > base.revision());
    assert!(
        !is_stale(&stale, AuthorityReason::ReplicaDivergence),
        "a standby with no base is not a divergent standby"
    );
    let recovery_generation = await_notice(
        &mut running.tap,
        "the surviving connection driving recovery",
        |notice| source_of(notice).and_then(recovering),
    )
    .await;
    assert_eq!(recovery_generation, standby_generation);

    assert_eq!(
        standby_peer.expect_resubscription(STEP_TIMEOUT).await.slugs,
        vec![SLUG.to_owned()],
        "the fresh base is withheld until the re-emit is observed on the wire, so the \
         resubscribe rail is proven end to end rather than assumed from a base that would \
         have arrived anyway"
    );

    standby_peer
        .send_orderbook(SLUG, &[("0.4", "300")], &[("0.7", "50")], Some(2))
        .await;
    let recovered = await_book(&mut running.observer, "the fresh venue base", is_live).await;
    assert_eq!(recovered.continuity().epoch(), 1);
    assert_eq!(
        levels_of(&recovered, Side::Bid),
        expect_levels(&[("0.4", "300")])
    );
    assert_strictly_increasing(&[base.revision(), stale.revision(), recovered.revision()]);

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 0);
    assert_eq!(stats.promotions_refused, 1);
    assert_eq!(stats.shadow_snapshots_applied, 0);
    assert_eq!(
        stats.resubscribes_emitted, 1,
        "the base that ended recovery followed the re-emit that asked for it"
    );
    assert_eq!(stats.resubscribes_escalated, 0);
}

#[tokio::test]
async fn a_reconverged_standby_becomes_promotable_again() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.4", "999")], &[("0.7", "111")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    await_standby(
        &mut running.tap,
        "the standby diverging on content",
        StandbyState::Divergent(DivergenceReason::ContentMismatch),
    )
    .await;

    standby_peer
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(2))
        .await;
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby re-converging on the published book",
        StandbyState::Agreeing,
    )
    .await;

    primary.drop_abruptly().await;

    let promoted = await_notice(
        &mut running.tap,
        "the re-converged standby taking over",
        |notice| {
            source_of(notice)
                .and_then(publishing)
                .filter(|generation| *generation == standby_generation)
        },
    )
    .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.5", "150")], &[("0.6", "200")], Some(3))
        .await;
    let after = await_book(
        &mut running.observer,
        "a snapshot committed by the promoted source",
        |published| published.revision() > base.revision(),
    )
    .await;
    assert!(is_live(&after));
    assert_eq!(
        after.revision(),
        base.revision() + 1,
        "re-convergence promoted seamlessly: no staleness revision was published"
    );
    assert_eq!(after.continuity().epoch(), 0);
    assert_eq!(
        after
            .provenance()
            .expect("a committed revision carries provenance")
            .connection()
            .generation(),
        promoted
    );

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 1);
    assert_eq!(stats.promotions_refused, 0);
    assert_eq!(stats.continuity_losses, 0);
}

#[tokio::test]
async fn a_replacement_connection_joins_as_a_standby_without_failback() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "200")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby agreeing with the published book",
        StandbyState::Agreeing,
    )
    .await;

    primary.drop_abruptly().await;
    let promoted = await_notice(&mut running.tap, "the standby taking over", |notice| {
        source_of(notice)
            .and_then(publishing)
            .filter(|generation| *generation == standby_generation)
    })
    .await;

    let mut history = Vec::new();
    let mut replacement = peer.next_connection().await;
    replacement.complete_handshake().await;
    replacement
        .send_orderbook(SLUG, &[("0.2", "5")], &[("0.8", "7")], Some(9))
        .await;
    let shadowed = await_notice_recording(
        &mut running.tap,
        &mut history,
        "the replacement's book landing in the shadow rather than the published state",
        |notice| {
            source_of(notice)
                .and_then(standby)
                .filter(|(_, state)| {
                    state == &StandbyState::Divergent(DivergenceReason::ContentMismatch)
                })
                .map(|(generation, _)| generation)
        },
    )
    .await;
    assert!(
        shadowed > promoted,
        "the replacement is a later generation than the promoted source"
    );
    let latest = running.observer.latest();
    assert_eq!(
        levels_of(&latest, Side::Bid),
        expect_levels(&[("0.5", "100")]),
        "the replacement's book never reached the published state"
    );
    assert_eq!(latest.revision(), base.revision());

    standby_peer
        .send_orderbook(SLUG, &[("0.5", "150")], &[("0.6", "200")], Some(2))
        .await;
    let after = await_book(
        &mut running.observer,
        "the promoted source still publishing",
        |published| published.revision() > base.revision(),
    )
    .await;
    assert_eq!(
        after
            .provenance()
            .expect("a committed revision carries provenance")
            .connection()
            .generation(),
        promoted,
        "the recovered replacement never took the publishing role"
    );
    drain_transitions(&mut running.tap, &mut history);
    let published_by: Vec<u64> = history.iter().filter_map(publishing).collect();
    assert!(
        !published_by.is_empty(),
        "no publishing transition was recorded after the takeover, so this window proves nothing"
    );
    for generation in published_by {
        assert_eq!(
            generation, promoted,
            "the publishing role churned back to a recovered connection"
        );
    }

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 1, "no second promotion happened");
    assert_eq!(stats.continuity_losses, 0);
}

#[tokio::test]
async fn equal_books_written_with_different_decimal_lexemes_agree_and_promote() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint());
    let (mut primary, mut standby_peer) = connect_pair(&mut peer, &mut running.tap).await;

    primary
        .send_orderbook(SLUG, &[("0.5", "120")], &[("0.6", "200")], Some(1))
        .await;
    standby_peer
        .send_orderbook(SLUG, &[("0.50", "120.0")], &[("0.60", "200.00")], Some(1))
        .await;
    let base = await_book(&mut running.observer, "the first accepted base", is_live).await;
    let standby_generation = await_standby(
        &mut running.tap,
        "the standby agreeing across differing decimal lexemes",
        StandbyState::Agreeing,
    )
    .await;

    primary.drop_abruptly().await;
    let promoted = await_notice(
        &mut running.tap,
        "the lexically different but economically equal standby taking over",
        |notice| {
            source_of(notice)
                .and_then(publishing)
                .filter(|generation| *generation == standby_generation)
        },
    )
    .await;

    standby_peer
        .send_orderbook(SLUG, &[("0.50", "150.0")], &[("0.60", "200.00")], Some(2))
        .await;
    let after = await_book(
        &mut running.observer,
        "a snapshot committed by the promoted source",
        |published| published.revision() > base.revision(),
    )
    .await;
    assert!(is_live(&after));
    assert_eq!(after.revision(), base.revision() + 1);
    assert_eq!(after.continuity().epoch(), 0);
    assert_eq!(
        after
            .provenance()
            .expect("a committed revision carries provenance")
            .connection()
            .generation(),
        promoted
    );
    assert_eq!(
        levels_of(&after, Side::Bid),
        expect_levels(&[("0.5", "150")]),
        "the venue's decimal lexemes normalize to one exact value"
    );

    let stats = running.finish().await;
    assert_eq!(stats.promotions, 1);
    assert_eq!(stats.continuity_losses, 0);
}
