#![forbid(unsafe_code)]

//! Pooled-publish contracts for the real `limitless::supervisor` running `pooled = true`,
//! driven against the scripted controlled peer.
//!
//! One peer serves every socket of the pool; each accepted connection is scripted
//! separately, so a test decides exactly what each socket delivers and in what order.
//! Which connection the daemon put in which socket is read from the diagnostics tap by
//! Engine.IO session id rather than assumed from accept ordering. Every book assertion
//! reads through a [`BookObserver`] attached to the supervisor's own writer.
//!
//! Drops are made observable without a book change by scripting them immediately before a
//! publishing frame *on the same socket*: one socket's frames reach the daemon in the order
//! it wrote them, so awaiting the later frame's publication proves the earlier frame was
//! decided first.

mod support;

use pm_ws::limitless::ORDERBOOK_UPDATE_DEDUP_KEY;
use pm_ws::limitless::supervisor::{
    Stopper, Supervisor, SupervisorConfig, SupervisorError, SupervisorNotice, SupervisorStats,
};
use pm_ws::{
    AuthorityState, BookObserver, DedupKey, DedupKeyDeclaration, DedupKeySemantics,
    MAX_POOL_SOCKETS, PoolDegradeReason, PoolError, PoolGate, PoolSocketState, PublishedBook,
    ReplicaRole, Side, SourceState,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const SLUG: &str = "btc-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const TAP_CAPACITY: usize = 1024;
const PATIENT_HEARTBEAT_MS: u64 = 60_000;

/// A peer whose announced heartbeat cadence is far longer than any test step, so a test
/// about pooled publishing cannot be disturbed by a heartbeat deadline.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// Production policy with the reconnect clock compressed and `sockets` connections, pooled
/// or not.
fn test_config(endpoint: String, sockets: usize, pooled: bool) -> SupervisorConfig {
    SupervisorConfig {
        endpoint,
        market: SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        replicas: sockets,
        pooled,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        max_recovery_attempts: 2,
        fenced_linger: Duration::from_secs(10),
        resubscribe_window: Duration::from_secs(30),
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
    fn start(endpoint: String, sockets: usize, pooled: bool) -> Self {
        let (tap_tx, tap) = mpsc::channel(TAP_CAPACITY);
        let mut supervisor = Supervisor::new(test_config(endpoint, sockets, pooled))
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
                "timed out waiting for {what}; last seen revision={} authority={:?} bid={:?}",
                latest.revision(),
                latest.authority(),
                best_bid(&latest)
            ),
            Ok(Err(_)) => panic!("the book writer went away while waiting for {what}"),
            Ok(Ok(_)) => {}
        }
    }
}

/// Consumes diagnostics until one notice yields a value, or panics naming what it was
/// waiting for.
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

fn source_of(notice: &SupervisorNotice) -> Option<&SourceState> {
    match notice {
        SupervisorNotice::SourceTransition { source } => Some(source),
        _ => None,
    }
}

/// The best bid's quantity, which every test in this file uses as the book state's name.
fn best_bid(published: &PublishedBook) -> Option<String> {
    published
        .canonical_levels()
        .iter()
        .filter(|level| level.side() == Side::Bid)
        .map(|level| level.quantity().value().canonical())
        .next_back()
}

fn holds_bid(size: &str) -> impl Fn(&PublishedBook) -> bool + '_ {
    move |published| best_bid(published).as_deref() == Some(size)
}

fn is_live(published: &PublishedBook) -> bool {
    published.authority() == &AuthorityState::Live
}

/// Accepts and completes every connection the supervisor dials, returning them in socket
/// order: the publishing slot first, then each standby role by the order the daemon
/// announced it.
///
/// A pool numbers its sockets by the same order, so the returned index is the socket index
/// the gate and the stats report.
async fn connect_sockets(
    peer: &mut ControlledPeer,
    tap: &mut mpsc::Receiver<SupervisorNotice>,
    sockets: usize,
) -> Vec<PeerConnection> {
    let mut accepted = Vec::new();
    for _ in 0..sockets {
        let mut connection = peer.next_connection().await;
        assert_eq!(
            connection.complete_handshake().await.slugs,
            vec![SLUG.to_owned()],
            "every socket subscribes the same desired market set"
        );
        accepted.push(connection);
    }
    let mut roles: BTreeMap<String, ReplicaRole> = BTreeMap::new();
    while roles.len() < sockets {
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
    accepted.sort_by_key(|connection| {
        match roles
            .get(&connection.engine_sid())
            .expect("the daemon announced a role for every accepted connection")
        {
            ReplicaRole::PublishingPrimary => 0usize,
            _ => 1usize,
        }
    });
    accepted
}

/// The generation of the connection currently named as this market's single publishing
/// primary, which a pooled topology never reports.
fn publishing(source: &SourceState) -> Option<u64> {
    source
        .publishing_primary()
        .map(|primary| primary.connection().generation())
}

#[tokio::test]
async fn a_two_socket_pool_publishes_each_book_state_exactly_once_and_never_rolls_back() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let (first, second) = sockets.split_at_mut(1);
    let (socket_a, socket_b) = (&mut first[0], &mut second[0]);
    let mut revisions = Vec::new();

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    revisions.push(
        await_book(
            &mut running.observer,
            "the pool's first publication",
            holds_bid("100"),
        )
        .await
        .revision(),
    );

    socket_b
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    socket_b
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    revisions.push(
        await_book(
            &mut running.observer,
            "the second socket's first publication, behind its own duplicate",
            holds_bid("200"),
        )
        .await
        .revision(),
    );

    socket_b
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    socket_b
        .send_orderbook(SLUG, &[("0.5", "300")], &[("0.6", "9")], Some(12))
        .await;
    revisions.push(
        await_book(
            &mut running.observer,
            "a publication behind a repeat of the published key",
            holds_bid("300"),
        )
        .await
        .revision(),
    );

    socket_a
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    socket_a
        .send_orderbook(SLUG, &[("0.5", "400")], &[("0.6", "9")], Some(13))
        .await;
    revisions.push(
        await_book(
            &mut running.observer,
            "a publication behind an arrival the book had already outrun",
            holds_bid("400"),
        )
        .await
        .revision(),
    );

    socket_b
        .send_orderbook(SLUG, &[("0.5", "400")], &[("0.6", "9")], Some(13))
        .await;
    socket_b
        .send_orderbook(SLUG, &[("0.5", "500")], &[("0.6", "9")], Some(14))
        .await;
    revisions.push(
        await_book(&mut running.observer, "the fifth state", holds_bid("500"))
            .await
            .revision(),
    );

    socket_a
        .send_orderbook(SLUG, &[("0.5", "500")], &[("0.6", "9")], Some(14))
        .await;
    socket_a
        .send_orderbook(SLUG, &[("0.5", "600")], &[("0.6", "9")], Some(15))
        .await;
    let last = await_book(&mut running.observer, "the sixth state", holds_bid("600")).await;
    revisions.push(last.revision());

    assert!(is_live(&last), "the pool never lost the book's authority");
    for pair in revisions.windows(2) {
        assert_eq!(
            pair[1],
            pair[0] + 1,
            "each published state cost exactly one revision, so nothing the gate dropped \
             reached the book: {revisions:?}"
        );
    }

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_published, 6,
        "six distinct book states, each published once"
    );
    assert_eq!(
        stats.pool_published_by_socket,
        vec![3, 3],
        "both sockets won arrivals; neither was a silent passenger"
    );
    assert_eq!(
        stats.pool_dedup_drops, 4,
        "four arrivals carried a key the book had already published"
    );
    assert_eq!(
        stats.pool_stale_drops, 1,
        "one arrival was behind the published key, which is skew rather than a violation"
    );
    assert_eq!(stats.pool_degraded, None, "nothing tripped the tripwire");
    assert_eq!(stats.snapshots_applied, 6);
    assert_eq!(
        stats.continuity_losses, 0,
        "publishing across sockets never broke the book's mutation stream"
    );
}

#[tokio::test]
async fn a_socket_whose_own_keys_go_backwards_degrades_the_pool_to_primary_and_standby() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let (first, second) = sockets.split_at_mut(1);
    let (socket_a, socket_b) = (&mut first[0], &mut second[0]);

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;
    socket_b
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    let published_by_b = await_book(
        &mut running.observer,
        "the second socket publishing the newest state",
        holds_bid("200"),
    )
    .await;

    socket_a
        .send_orderbook(SLUG, &[("0.5", "999")], &[("0.6", "9")], Some(9))
        .await;

    let violation = await_notice(
        &mut running.tap,
        "the pool degrading",
        |notice| match notice {
            SupervisorNotice::PoolDegraded { violation, .. } => Some(violation.clone()),
            _ => None,
        },
    )
    .await;
    assert_eq!(violation.reason, PoolDegradeReason::ConnectionInversion);
    assert_eq!(
        violation.socket, 0,
        "the socket that contradicted its own declared ordering is named"
    );
    assert_eq!(
        violation.observed_key.map(|key| key.to_string()),
        Some("9".to_owned())
    );
    assert_eq!(
        violation.previous_key.map(|key| key.to_string()),
        Some("10".to_owned()),
        "the key that connection had already delivered is kept, not summarized"
    );

    let promoted = await_notice(
        &mut running.tap,
        "the topology handed back to one publishing primary",
        |notice| source_of(notice).and_then(publishing),
    )
    .await;
    let after_degrade = running.observer.latest();
    assert_eq!(
        best_bid(&after_degrade).as_deref(),
        Some("200"),
        "the arrival that withdrew the licence reached no book"
    );
    assert_eq!(
        after_degrade.revision(),
        published_by_b.revision(),
        "the degrade itself published nothing"
    );
    assert!(
        is_live(&after_degrade),
        "handing the book back to one primary is not a loss of authority"
    );
    assert_eq!(
        after_degrade.continuity().epoch(),
        base.continuity().epoch(),
        "the degrade opens no continuity epoch"
    );

    socket_b
        .send_orderbook(SLUG, &[("0.5", "300")], &[("0.6", "9")], Some(12))
        .await;
    let resumed = await_book(
        &mut running.observer,
        "the degraded topology still publishing",
        holds_bid("300"),
    )
    .await;
    assert_eq!(
        resumed
            .provenance()
            .expect("a committed revision carries provenance")
            .connection()
            .generation(),
        promoted,
        "the socket holding the last published state is the one that took the primary role"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::ConnectionInversion)
    );
    assert_eq!(
        stats.pool_published, 2,
        "the pool published twice before it withdrew its own licence"
    );
    assert_eq!(stats.continuity_losses, 0);
    assert_eq!(stats.promotions_refused, 0);
}

#[tokio::test]
async fn equal_keys_carrying_different_content_degrade_the_pool_to_primary_and_standby() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let (first, second) = sockets.split_at_mut(1);
    let (socket_a, socket_b) = (&mut first[0], &mut second[0]);

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;

    socket_b
        .send_orderbook(SLUG, &[("0.5", "999")], &[("0.6", "9")], Some(10))
        .await;

    let violation = await_notice(
        &mut running.tap,
        "the pool degrading",
        |notice| match notice {
            SupervisorNotice::PoolDegraded { violation, .. } => Some(violation.clone()),
            _ => None,
        },
    )
    .await;
    assert_eq!(
        violation.reason,
        PoolDegradeReason::EqualKeyContentMismatch,
        "one key naming two different book states is not the equality a pool dedups on"
    );
    assert_eq!(violation.socket, 1);
    assert_ne!(
        violation.observed_digest,
        violation
            .previous_digest
            .expect("the content already seen under that key is kept"),
        "the two fingerprints under one key are recorded, not just the conclusion"
    );

    let primary = await_notice(
        &mut running.tap,
        "the topology handed back to one publishing primary",
        |notice| source_of(notice).and_then(publishing),
    )
    .await;
    let after_degrade = running.observer.latest();
    assert_eq!(
        best_bid(&after_degrade).as_deref(),
        Some("100"),
        "the arrival that withdrew the licence reached no book"
    );
    assert_eq!(after_degrade.revision(), base.revision());
    assert!(is_live(&after_degrade));

    socket_a
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    let resumed = await_book(
        &mut running.observer,
        "the degraded topology still publishing",
        holds_bid("200"),
    )
    .await;
    assert_eq!(
        resumed
            .provenance()
            .expect("a committed revision carries provenance")
            .connection()
            .generation(),
        primary,
        "the socket holding the last published state kept the primary role"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::EqualKeyContentMismatch)
    );
    assert_eq!(stats.continuity_losses, 0);
}

#[tokio::test]
async fn killing_the_publishing_socket_leaves_the_pool_publishing_without_a_gap() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let mut socket_b = sockets.pop().expect("the pool holds two sockets");
    let mut socket_a = sockets.pop().expect("the pool holds two sockets");

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;

    socket_a.drop_abruptly().await;

    socket_b
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    let survived = await_book(
        &mut running.observer,
        "the surviving socket publishing after the kill",
        holds_bid("200"),
    )
    .await;
    assert!(
        is_live(&survived),
        "losing one socket of a pool is a loss of coverage, never of authority"
    );
    assert_eq!(
        survived.revision(),
        base.revision() + 1,
        "a staleness report would have spent a revision between the two states"
    );
    assert_eq!(
        survived.continuity().epoch(),
        base.continuity().epoch(),
        "no continuity epoch opened across the kill"
    );

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        vec![SLUG.to_owned()],
        "the pool rebuilds its coverage with the same desired market set"
    );
    let covering = await_notice(
        &mut running.tap,
        "the pool reporting full socket coverage again",
        |notice| {
            source_of(notice)
                .filter(|source| source.pool_capacity() == 2 && source.pool_covering() == 2)
                .map(|source| source.pool_sockets().count())
        },
    )
    .await;
    assert_eq!(covering, 2);

    replacement
        .send_orderbook(SLUG, &[("0.5", "300")], &[("0.6", "9")], Some(12))
        .await;
    let rebuilt = await_book(
        &mut running.observer,
        "the replacement socket publishing into the same pool",
        holds_bid("300"),
    )
    .await;
    assert!(is_live(&rebuilt));
    assert_eq!(rebuilt.revision(), survived.revision() + 1);

    let stats = running.finish().await;
    assert_eq!(
        stats.continuity_losses, 0,
        "the publishing socket died and the book never lost authority"
    );
    assert_eq!(
        stats.promotions_refused, 0,
        "a pool asks no promotion question, so it can refuse none"
    );
    assert_eq!(stats.pool_published, 3);
    assert_eq!(stats.pool_degraded, None);
}

#[tokio::test]
async fn killing_the_other_pool_socket_leaves_the_publishing_socket_untouched() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let socket_b = sockets.pop().expect("the pool holds two sockets");
    let mut socket_a = sockets.pop().expect("the pool holds two sockets");

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;

    socket_b.drop_abruptly().await;

    socket_a
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    let survived = await_book(
        &mut running.observer,
        "the publishing socket carrying on after the other died",
        holds_bid("200"),
    )
    .await;
    assert!(is_live(&survived));
    assert_eq!(survived.revision(), base.revision() + 1);

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );
    let covering = await_notice(
        &mut running.tap,
        "the pool reporting full socket coverage again",
        |notice| {
            source_of(notice)
                .filter(|source| source.pool_capacity() == 2 && source.pool_covering() == 2)
                .map(|source| {
                    source
                        .pool_sockets()
                        .filter(|(_, state)| matches!(state, PoolSocketState::Covering))
                        .count()
                })
        },
    )
    .await;
    assert_eq!(covering, 2);

    let stats = running.finish().await;
    assert_eq!(stats.continuity_losses, 0);
    assert_eq!(stats.pool_published, 2);
    assert_eq!(stats.pool_published_by_socket, vec![2, 0]);
}

#[tokio::test]
async fn without_the_pool_flag_a_second_connection_publishes_nothing() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, false);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let (first, second) = sockets.split_at_mut(1);
    let (primary, standby) = (&mut first[0], &mut second[0]);

    primary
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(10))
        .await;
    let base = await_book(
        &mut running.observer,
        "the primary's base",
        holds_bid("100"),
    )
    .await;

    standby
        .send_orderbook(SLUG, &[("0.5", "999")], &[("0.6", "9")], Some(99))
        .await;
    primary
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(11))
        .await;
    let after = await_book(
        &mut running.observer,
        "the primary's next snapshot",
        holds_bid("200"),
    )
    .await;
    assert_eq!(
        after.revision(),
        base.revision() + 1,
        "the standby's newer key published nothing, so it spent no revision"
    );
    let capacity = await_notice(
        &mut running.tap,
        "a source topology this run reported",
        |notice| source_of(notice).map(SourceState::pool_capacity),
    )
    .await;
    assert_eq!(
        capacity, 0,
        "a configuration without the flag reports no pool at all"
    );

    let stats = running.finish().await;
    assert_eq!(stats.pool_published, 0);
    assert_eq!(
        stats.pool_published_by_socket,
        Vec::<u64>::new(),
        "no pool ran, so there are no per-socket counts to report"
    );
    assert_eq!(stats.pool_dedup_drops, 0);
    assert_eq!(stats.pool_stale_drops, 0);
    assert_eq!(stats.pool_degraded, None);
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(stats.shadow_snapshots_applied, 1);
}

/// Accepts one connection, completes its handshake, and returns it with the role the daemon
/// announced for it, without waiting for any other socket to arrive.
async fn connect_one(
    peer: &mut ControlledPeer,
    tap: &mut mpsc::Receiver<SupervisorNotice>,
) -> (PeerConnection, ReplicaRole) {
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );
    let sid = connection.engine_sid();
    let role = await_notice(
        tap,
        "this connection's announced role",
        |notice| match notice {
            SupervisorNotice::Connected {
                sid: announced,
                replica,
                ..
            } if announced == &sid => Some(replica.clone()),
            _ => None,
        },
    )
    .await;
    (connection, role)
}

#[tokio::test]
async fn a_pools_second_socket_joining_behind_the_published_key_is_skew_and_not_a_rewind() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);

    let (mut socket_a, role_a) = connect_one(&mut peer, &mut running.tap).await;
    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(20))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;

    let (mut socket_b, role_b) = connect_one(&mut peer, &mut running.tap).await;
    assert_ne!(
        role_a, role_b,
        "the pool announces one publishing slot and one standby; which socket arrives first is scheduling"
    );
    socket_b
        .send_orderbook(SLUG, &[("0.5", "999")], &[("0.6", "9")], Some(5))
        .await;
    socket_b
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(21))
        .await;
    let after = await_book(
        &mut running.observer,
        "the joining socket's next publication, behind its own late frame",
        holds_bid("200"),
    )
    .await;
    assert_eq!(
        after.revision(),
        base.revision() + 1,
        "the late frame reached no book"
    );
    assert!(is_live(&after));

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded, None,
        "a socket joining an already-publishing pool makes no claim about a counter \
         continuing, so its first key falling behind is skew and not a rewind"
    );
    assert_eq!(stats.pool_stale_drops, 1);
}

#[tokio::test]
async fn a_replacement_socket_whose_first_key_falls_below_the_published_floor_degrades_the_pool() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let socket_b = sockets.pop().expect("the pool holds two sockets");
    let mut socket_a = sockets.pop().expect("the pool holds two sockets");

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(20))
        .await;
    let base = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;

    socket_b.drop_abruptly().await;
    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        vec![SLUG.to_owned()]
    );

    replacement
        .send_orderbook(SLUG, &[("0.5", "999")], &[("0.6", "9")], Some(5))
        .await;

    let violation = await_notice(
        &mut running.tap,
        "the pool degrading",
        |notice| match notice {
            SupervisorNotice::PoolDegraded { violation, .. } => Some(violation.clone()),
            _ => None,
        },
    )
    .await;
    assert_eq!(
        violation.reason,
        PoolDegradeReason::ReconnectRewind,
        "the recorded conformance has the counter continuing across a reconnect, so a \
         replacement session starting below the published key contradicts it"
    );
    assert_eq!(
        violation.published_key.map(|key| key.to_string()),
        Some("20".to_owned()),
        "the floor the first key fell below is named"
    );
    let after = running.observer.latest();
    assert_eq!(
        best_bid(&after).as_deref(),
        Some("100"),
        "the arrival that withdrew the licence reached no book"
    );
    assert_eq!(after.revision(), base.revision());
    assert!(is_live(&after));

    socket_a
        .send_orderbook(SLUG, &[("0.5", "200")], &[("0.6", "9")], Some(21))
        .await;
    let resumed = await_book(
        &mut running.observer,
        "the degraded topology still publishing",
        holds_bid("200"),
    )
    .await;
    assert!(is_live(&resumed));

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::ReconnectRewind)
    );
    assert_eq!(stats.continuity_losses, 0);
}

#[tokio::test]
async fn a_pool_advancing_its_published_key_reports_it_without_a_transition_per_update() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut running = Running::start(peer.endpoint(), 2, true);
    let mut sockets = connect_sockets(&mut peer, &mut running.tap, 2).await;
    let mut socket_a = sockets.remove(0);

    socket_a
        .send_orderbook(SLUG, &[("0.5", "100")], &[("0.6", "9")], Some(30))
        .await;
    let _ = await_book(
        &mut running.observer,
        "the pool's first publication",
        holds_bid("100"),
    )
    .await;
    while running.tap.try_recv().is_ok() {}

    for (size, version) in [("200", 31u64), ("300", 32), ("400", 33)] {
        socket_a
            .send_orderbook(SLUG, &[("0.5", size)], &[("0.6", "9")], Some(version))
            .await;
        let _ = await_book(
            &mut running.observer,
            "a later publication",
            holds_bid(size),
        )
        .await;
    }

    let mut transitions = 0usize;
    while let Ok(notice) = running.tap.try_recv() {
        if source_of(&notice).is_some() {
            transitions += 1;
        }
    }
    assert_eq!(
        transitions, 0,
        "a published key advancing is latest state, not a topology transition; reporting \
         one per update would put a diagnostic notice on the update path"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_last_published_key,
        Some(DedupKey::integer(33)),
        "the key the run ended on is reachable from the summary the operator reads"
    );
    assert_eq!(stats.pool_published, 4);
}

#[test]
fn a_pool_is_refused_for_a_venue_key_that_orders_nothing_within_a_session() {
    assert!(
        ORDERBOOK_UPDATE_DEDUP_KEY
            .semantics()
            .admits_pooled_publish(),
        "this venue's recorded conformance is what lets a pool be opted into at all"
    );
    let dedup_only = DedupKeyDeclaration::new(
        "limitless",
        "orderbookUpdate",
        "version",
        DedupKeySemantics::DedupOnly,
    );
    assert_eq!(
        PoolGate::new(dedup_only, 2).err(),
        Some(PoolError::KeyOrdersNothing(DedupKeySemantics::DedupOnly)),
        "a key that orders nothing within a session can neither choose the newer arrival \
         nor recognize a violation, so it is refused rather than gated on"
    );
}

#[test]
fn a_pooled_supervisor_refuses_a_socket_count_outside_the_pool_gate() {
    let config = |sockets| SupervisorConfig {
        market: SLUG.to_owned(),
        replicas: sockets,
        pooled: true,
        ..SupervisorConfig::default()
    };
    for sockets in [0, 1, MAX_POOL_SOCKETS + 1] {
        assert_eq!(
            Supervisor::new(config(sockets)).err(),
            Some(SupervisorError::Pool(PoolError::SocketCountOutOfRange)),
            "a pool of {sockets} sockets was accepted"
        );
    }
    assert!(
        Supervisor::new(config(MAX_POOL_SOCKETS)).is_ok(),
        "the pool gate's own structural ceiling is what bounds the pool, and it admits this one"
    );
}
