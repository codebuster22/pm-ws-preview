#![forbid(unsafe_code)]

//! Pooled-publishing contracts for the real `limitless::shard` running the daemon's
//! redundant topology, driven against the scripted controlled peer.
//!
//! `replicas > 1` runs the shard's connections active-active through the venue-key gate
//! wherever the venue's dedup-key declaration admits pooled publishing. Every connection
//! subscribes the whole desired set; the first arrival whose key passes the market's last
//! published key publishes whichever socket carried it; an equal key is the same frame
//! arriving again and is dropped once its content has been compared exactly; a lower key is
//! cross-connection skew and is dropped stale. Losing a socket costs coverage and asks no
//! promotion question.
//!
//! The licence rests on the conformance basis recorded in `docs/limitless.md`, so it is
//! carried live: the first arrival contradicting that basis — or leaving it uncheckable —
//! withdraws it for the rest of the process and hands the shard back to one publishing
//! primary with hot standbys. `shard_replica_contracts.rs` holds that topology's own
//! contracts; what is here is the armed pool, the boundary between the two, and the shard
//! behaving as a pool on either side of it.
//!
//! Sockets are named by the pool socket index the shard reports for each Engine.IO session
//! id, never by the order the listener accepted them: every role dials on its own schedule,
//! so accept order is the runtime's business and not a property of the shard.

mod support;

use pm_ws::limitless::shard::{
    MarketStatus, PoolState, Shard, ShardConfig, ShardHandle, ShardMetrics, ShardStats,
    ShardStatus, ShardStopper,
};
use pm_ws::{
    AuthorityReason, AuthorityState, BookObserver, DedupKeyDeclaration, DedupKeySemantics,
    MAX_POOL_SOCKETS, MutationContinuity, PoolDegradeReason, PublishedBook, ReplicaRole, Side,
};
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const MARKET_A: &str = "btc-up-or-down-5-min-1788172500";
const MARKET_B: &str = "eth-up-or-down-5-min-1788172500";
const MARKET_C: &str = "sol-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
/// Long enough that a publication which was going to happen has happened, so a negative
/// assertion about the book is evidence rather than a race.
const QUIET_WINDOW: Duration = Duration::from_millis(400);
/// How many shards a contract needing a particular socket layout will build before giving up.
///
/// Which of a shard's roles dials first is the runtime's business, and a contract about one
/// role does not get to assert the other one's timing. A scene that needs a layout the shard
/// did not produce is abandoned whole and set again on a fresh shard, which is the only way
/// to ask for one without depending on accept order.
const LAYOUT_ATTEMPTS: usize = 4;

/// A venue key declaration whose semantics grant no ordering at all, which is the tier
/// `docs/design.md` gives a venue that has earned no pooled-publishing licence.
const DEDUP_ONLY: DedupKeyDeclaration = DedupKeyDeclaration::new(
    "limitless",
    "orderbookUpdate",
    "version",
    DedupKeySemantics::DedupOnly,
);

fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// Production policy with the reconnect clock compressed.
fn single_config(endpoint: String, markets: &[&str]) -> ShardConfig {
    ShardConfig {
        endpoint,
        markets: markets.iter().map(|slug| (*slug).to_owned()).collect(),
        setup_timeout: Duration::from_secs(10),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        max_recovery_attempts: 4,
        fenced_linger: Duration::from_secs(10),
        resubscribe_window: Duration::from_secs(30),
        observer_capacity: 256,
        ..ShardConfig::default()
    }
}

/// Two connections on the venue's own declaration, which is the configuration that arms a
/// pool.
fn pool_config(endpoint: String, markets: &[&str]) -> ShardConfig {
    ShardConfig {
        replicas: 2,
        ..single_config(endpoint, markets)
    }
}

struct Running {
    handle: ShardHandle,
    stop: ShardStopper,
    task: JoinHandle<ShardStats>,
}

impl Running {
    fn start(shard: Shard) -> Self {
        let handle = shard.handle();
        let stop = shard.stopper();
        let task = tokio::spawn(async move {
            let mut shard = shard;
            shard.run_until(Some(Instant::now() + RUN_CAP)).await
        });
        Self { handle, stop, task }
    }

    async fn status(&self) -> ShardStatus {
        self.handle.status().await.expect("the shard is running")
    }

    async fn metrics(&self) -> ShardMetrics {
        self.handle.metrics().await.expect("the shard is running")
    }

    async fn finish(self) -> ShardStats {
        self.stop.stop();
        tokio::time::timeout(STEP_TIMEOUT, self.task)
            .await
            .expect("shard run ends after stop")
            .expect("shard task completes")
    }
}

fn is_live(published: &PublishedBook) -> bool {
    matches!(published.authority(), AuthorityState::Live)
}

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

async fn await_status(
    running: &Running,
    what: &str,
    predicate: impl Fn(&ShardStatus) -> bool,
) -> ShardStatus {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let status = running.status().await;
        if predicate(&status) {
            return status;
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for {what}; last saw pool={:?} connections={:?}",
                status.pool, status.connections
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn await_metrics(
    running: &Running,
    what: &str,
    predicate: impl Fn(&ShardMetrics) -> bool,
) -> ShardMetrics {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let metrics = running.metrics().await;
        if predicate(&metrics) {
            return metrics;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {what}; last saw {:?}", metrics.stats);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn levels(published: &PublishedBook, side: Side) -> Vec<(String, String)> {
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

fn owned(slugs: &[&str]) -> Vec<String> {
    slugs.iter().map(|slug| (*slug).to_owned()).collect()
}

fn status_of(status: &ShardStatus, slug: &str) -> Option<MarketStatus> {
    status
        .markets
        .iter()
        .find(|report| report.slug == slug)
        .map(|report| report.status.clone())
}

/// The pool socket the shard reports for the connection holding `session`, once it has one.
fn socket_of(status: &ShardStatus, session: &str) -> Option<usize> {
    status
        .connections
        .iter()
        .find(|row| row.session.as_deref() == Some(session))
        .and_then(|row| row.pool_socket)
}

/// The role the shard reports for the connection holding `session`, once it has one.
fn role_of(status: &ShardStatus, session: &str) -> Option<ReplicaRole> {
    status
        .connections
        .iter()
        .find(|row| row.session.as_deref() == Some(session))
        .map(|row| row.role.clone())
}

fn pool_state(status: &ShardStatus) -> Option<PoolState> {
    status.pool.as_ref().map(|pool| pool.state.clone())
}

/// Sends one distinguishable book for `slug` under `version`.
async fn send_book(connection: &mut PeerConnection, slug: &str, size: &str, version: u64) {
    connection
        .send_orderbook(slug, &[("0.51", size)], &[("0.52", size)], Some(version))
        .await;
}

/// Sends one book for `slug` carrying no `version` at all, which is the venue withdrawing
/// the key the gate runs on.
async fn send_keyless_book(connection: &mut PeerConnection, slug: &str, size: &str) {
    connection
        .send_orderbook(slug, &[("0.51", size)], &[("0.52", size)], None)
        .await;
}

/// Accepts and completes both of the shard's connections, then returns them ordered by the
/// pool socket index the shard reported for each.
async fn connect_pool(
    peer: &mut ControlledPeer,
    running: &Running,
    markets: &[&str],
) -> (PeerConnection, PeerConnection) {
    let mut first = peer.next_connection().await;
    assert_eq!(
        first.complete_handshake().await.slugs,
        owned(markets),
        "the first pooled connection subscribes the whole desired set"
    );
    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        owned(markets),
        "every pooled connection subscribes the same replace-set"
    );
    let first_sid = first.engine_sid();
    let second_sid = second.engine_sid();
    let status = await_status(
        running,
        "both connections to take a pool socket",
        |status| {
            socket_of(status, &first_sid).is_some() && socket_of(status, &second_sid).is_some()
        },
    )
    .await;
    let first_socket = socket_of(&status, &first_sid).expect("the shard named the first socket");
    let second_socket = socket_of(&status, &second_sid).expect("the shard named the second socket");
    assert_ne!(
        first_socket, second_socket,
        "two connections never hold one pool socket"
    );
    if first_socket < second_socket {
        (first, second)
    } else {
        (second, first)
    }
}

/// (a) The pool publishes the first arrival and drops the second delivery of that same
/// frame, without spending a revision on it.
///
/// This is what a pool is for: two sockets carry the same stream, the faster one is the one
/// the book takes, and the slower one costs the book nothing. Equal keys are compared
/// exactly before being dropped, so the drop is evidence that the venue delivered the same
/// frame twice rather than an assumption that it did.
#[tokio::test]
async fn the_first_socket_to_deliver_a_frame_publishes_it_and_the_second_delivery_is_dropped() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    assert_eq!(
        pool_state(&running.status().await),
        Some(PoolState::Armed),
        "the venue's own declaration admits pooled publishing"
    );

    send_book(&mut socket_zero, MARKET_A, "10", 100).await;
    let published = await_book(&mut book, "the first arrival to publish", is_live).await;
    assert_eq!(
        levels(&published, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the book carries what the first socket delivered"
    );

    send_book(&mut socket_one, MARKET_A, "10", 100).await;
    await_metrics(&running, "the duplicate to be judged", |metrics| {
        metrics.stats.pool_duplicate_drops == 1
    })
    .await;
    tokio::time::sleep(QUIET_WINDOW).await;

    let after = book.latest();
    assert_eq!(
        after.revision(),
        published.revision(),
        "the same frame arriving again spends no revision"
    );
    assert!(
        matches!(
            after.continuity(),
            MutationContinuity::Intact { epoch: 0, .. }
        ),
        "a duplicate opens no epoch: {:?}",
        after.continuity()
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_published, 1,
        "one of the two deliveries reached the book"
    );
    assert_eq!(
        stats.pool_duplicate_drops, 1,
        "the other was dropped as the duplicate it is"
    );
    assert_eq!(stats.pool_stale_drops, 0, "neither arrival was skew");
    assert_eq!(
        stats.pool_published_by_socket,
        vec![1, 0],
        "the socket that carried the published arrival is the one credited"
    );
}

/// (b) An arrival below the published key is cross-connection skew, not a violation.
#[tokio::test]
async fn a_lower_key_arriving_late_on_another_socket_is_dropped_stale() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    send_book(&mut socket_zero, MARKET_A, "10", 200).await;
    let published = await_book(&mut book, "the newer arrival to publish", is_live).await;

    send_book(&mut socket_one, MARKET_A, "99", 150).await;
    await_metrics(&running, "the skewed arrival to be judged", |metrics| {
        metrics.stats.pool_stale_drops == 1
    })
    .await;
    tokio::time::sleep(QUIET_WINDOW).await;

    let after = book.latest();
    assert_eq!(
        after.revision(),
        published.revision(),
        "an arrival the book has already passed spends no revision"
    );
    assert_eq!(
        levels(&after, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the published book still holds the newer state"
    );
    assert_eq!(
        pool_state(&running.status().await),
        Some(PoolState::Armed),
        "ordinary skew is not a violation of anything and withdraws no licence"
    );

    let stats = running.finish().await;
    assert_eq!(stats.pool_stale_drops, 1, "one arrival was dropped as skew");
    assert_eq!(stats.pool_duplicate_drops, 0, "skew is not a duplicate");
    assert_eq!(stats.pool_degraded, None, "the licence still stands");
}

/// (c) One key naming two different book states withdraws the licence, and the book it was
/// judged against is left exactly as it stood.
#[tokio::test]
async fn one_key_carrying_two_contents_withdraws_the_licence_and_touches_no_book() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    send_book(&mut socket_zero, MARKET_A, "10", 300).await;
    let published = await_book(&mut book, "the first arrival to publish", is_live).await;

    send_book(&mut socket_one, MARKET_A, "77", 300).await;
    let degraded = await_status(&running, "the tripwire to withdraw the licence", |status| {
        !matches!(pool_state(status), Some(PoolState::Armed))
    })
    .await;
    assert_eq!(
        pool_state(&degraded),
        Some(PoolState::Degraded {
            reason: PoolDegradeReason::EqualKeyContentMismatch
        }),
        "the withdrawal names the observation that caused it"
    );

    tokio::time::sleep(QUIET_WINDOW).await;
    let after = book.latest();
    assert_eq!(
        after.revision(),
        published.revision(),
        "the arrival that withdrew the licence reached no book"
    );
    assert!(
        is_live(&after),
        "the hand-back replaces no published state: {:?}",
        after.authority()
    );
    assert!(
        matches!(
            after.continuity(),
            MutationContinuity::Intact { epoch: 0, .. }
        ),
        "the hand-back opens no continuity epoch: {:?}",
        after.continuity()
    );
    assert_eq!(
        levels(&after, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the book still holds the state the pool published"
    );

    let primary_sid = degraded
        .connections
        .iter()
        .find(|row| row.role == ReplicaRole::PublishingPrimary)
        .and_then(|row| row.session.clone())
        .expect("a publishing primary is named after the hand-back");
    let mut publisher = if publisher_is(&mut socket_zero, &primary_sid) {
        socket_zero
    } else {
        socket_one
    };
    send_book(&mut publisher, MARKET_A, "12", 301).await;
    let advanced = await_book(
        &mut book,
        "the primary to publish after the hand-back",
        |b| b.revision() > after.revision(),
    )
    .await;
    assert!(
        advanced.revision() > after.revision(),
        "the shard keeps publishing under one primary"
    );
    assert!(
        matches!(
            advanced.continuity(),
            MutationContinuity::Intact { epoch: 0, .. }
        ),
        "monotonic and continuous across the hand-back: {:?}",
        advanced.continuity()
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::EqualKeyContentMismatch),
        "the run's counters name the reason the licence was withdrawn"
    );
}

fn publisher_is(connection: &mut PeerConnection, session: &str) -> bool {
    connection.engine_sid() == session
}

/// (d) Losing one pooled socket costs coverage and nothing else: no promotion question, no
/// gap, and the replacement rejoins the pool.
#[tokio::test]
async fn losing_a_pooled_socket_asks_no_promotion_question_and_the_replacement_rejoins() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut books = [
        shard.observe(MARKET_A).expect("market A is in the set"),
        shard.observe(MARKET_B).expect("market B is in the set"),
    ];
    let running = Running::start(shard);
    let (socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    for (index, slug) in markets.iter().enumerate() {
        send_book(&mut socket_one, slug, "10", 400 + index as u64).await;
    }
    let mut bases = Vec::new();
    for (index, slug) in markets.iter().enumerate() {
        bases.push(await_book(&mut books[index], &format!("{slug} live"), is_live).await);
    }

    socket_zero.drop_abruptly().await;

    for (index, slug) in markets.iter().enumerate() {
        send_book(&mut socket_one, slug, "20", 410 + index as u64).await;
    }
    for (index, slug) in markets.iter().enumerate() {
        let after = await_book(
            &mut books[index],
            &format!("{slug} to keep publishing on the surviving socket"),
            |published| published.revision() > bases[index].revision(),
        )
        .await;
        assert!(
            is_live(&after),
            "{slug} never left live when a socket went away: {:?}",
            after.authority()
        );
        assert_eq!(
            after.revision(),
            bases[index].revision() + 1,
            "{slug}: a staleness report would have spent a revision between the two"
        );
        assert!(
            matches!(
                after.continuity(),
                MutationContinuity::Intact { epoch: 0, .. }
            ),
            "{slug}: losing pool coverage opens no epoch: {:?}",
            after.continuity()
        );
    }

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&markets),
        "the replacement rejoins the pool on the whole desired set"
    );
    let replacement_sid = replacement.engine_sid();
    await_status(
        &running,
        "the replacement to take a pool socket",
        |status| socket_of(status, &replacement_sid).is_some(),
    )
    .await;
    assert_eq!(
        pool_state(&running.status().await),
        Some(PoolState::Armed),
        "a socket coming and going is not a violation of the recorded basis"
    );

    send_book(&mut replacement, MARKET_A, "30", 420).await;
    let from_replacement = await_book(&mut books[0], "the replacement to publish", |published| {
        levels(published, Side::Bid) == expect_levels(&[("0.51", "30")])
    })
    .await;
    assert!(
        is_live(&from_replacement),
        "the replacement publishes as a pool socket"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.source_switches, 0,
        "an armed pool asks no promotion question"
    );
    assert_eq!(stats.markets_promoted, 0, "nothing was promoted");
    assert_eq!(
        stats.markets_promotion_refused, 0,
        "and nothing was refused"
    );
    assert_eq!(
        stats.continuity_losses, 0,
        "the survivors kept the books' continuity intact"
    );
    assert_eq!(
        stats.pool_handovers, 1,
        "the publishing slot was refilled from a surviving socket"
    );
    assert_eq!(stats.pool_degraded, None, "the licence still stands");
}

/// (e) The shipped default is one connection, and the shard it produces says nothing about
/// a pool anywhere on its surfaces.
#[tokio::test]
async fn the_default_configuration_reports_no_pool_at_all() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(single_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let mut only = peer.next_connection().await;
    assert_eq!(only.complete_handshake().await.slugs, owned(&markets));

    send_book(&mut only, MARKET_A, "10", 500).await;
    await_book(&mut book, "the single connection to publish", is_live).await;

    let status = running.status().await;
    assert!(
        status.pool.is_none(),
        "a shard running one connection holds no pool: {:?}",
        status.pool
    );
    assert!(
        status
            .connections
            .iter()
            .all(|row| row.pool_socket.is_none()),
        "no connection of a single-connection shard occupies a pool socket"
    );
    let encoded = serde_json::to_string(&status).expect("a shard status encodes");
    assert!(
        !encoded.contains("pool"),
        "the default topology writes the status shape it wrote before pooling existed: {encoded}"
    );

    let metrics = running.metrics().await;
    assert!(
        metrics.pool.is_none(),
        "and reports no pool to a scrape either"
    );

    // A frame with no venue key is nothing but an ordinary frame to a shard that gates on
    // none, so it publishes exactly as it always did.
    send_keyless_book(&mut only, MARKET_A, "20").await;
    let after = await_book(
        &mut book,
        "a keyless frame to publish unchanged",
        |published| levels(published, Side::Bid) == expect_levels(&[("0.51", "20")]),
    )
    .await;
    assert!(is_live(&after), "the default rail gates on no key");

    let stats = running.finish().await;
    assert_eq!(stats.pool_published, 0, "nothing was published by a pool");
    assert_eq!(stats.pool_degraded, None, "and no licence was ever held");
    assert!(
        stats.pool_published_by_socket.is_empty(),
        "a shard with no pool credits no socket"
    );
}

/// (g) A venue whose declaration admits no pooled publishing runs its redundant
/// connections as one primary with hot standbys from the start, and says why.
#[tokio::test]
async fn a_venue_key_that_orders_nothing_runs_the_replicas_as_standbys_from_the_start() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let config = ShardConfig {
        dedup_key: DEDUP_ONLY,
        ..pool_config(peer.endpoint(), &markets)
    };
    let shard = Shard::new(config).expect("a shard on an unlicensed key is still a shard");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut first = peer.next_connection().await;
    assert_eq!(first.complete_handshake().await.slugs, owned(&markets));
    let mut second = peer.next_connection().await;
    assert_eq!(second.complete_handshake().await.slugs, owned(&markets));

    let first_sid = first.engine_sid();
    let second_sid = second.engine_sid();
    let status = await_status(&running, "both connections to announce a role", |status| {
        role_of(status, &first_sid).is_some() && role_of(status, &second_sid).is_some()
    })
    .await;
    assert_eq!(
        pool_state(&status),
        Some(PoolState::Unlicensed {
            semantics: DedupKeySemantics::DedupOnly.as_label().to_owned()
        }),
        "the shard names the declaration that withheld the licence"
    );
    assert!(
        status
            .connections
            .iter()
            .all(|row| row.pool_socket.is_none()),
        "an unlicensed topology holds no pool sockets: {:?}",
        status.connections
    );
    let roles: Vec<ReplicaRole> = status
        .connections
        .iter()
        .map(|row| row.role.clone())
        .collect();
    assert_eq!(
        roles,
        vec![ReplicaRole::PublishingPrimary, ReplicaRole::HotStandby],
        "one publishing primary and one hot standby"
    );

    let (mut primary, mut standby) =
        if role_of(&status, &first_sid) == Some(ReplicaRole::PublishingPrimary) {
            (first, second)
        } else {
            (second, first)
        };
    send_book(&mut primary, MARKET_A, "10", 600).await;
    let published = await_book(&mut book, "the primary to publish", is_live).await;

    send_book(&mut standby, MARKET_A, "40", 601).await;
    tokio::time::sleep(QUIET_WINDOW).await;
    let after = book.latest();
    assert_eq!(
        after.revision(),
        published.revision(),
        "a standby publishes nothing when no key licenses it to"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_published, 0,
        "no arrival was ever chosen by a key gate"
    );
    assert_eq!(
        stats.pool_degraded, None,
        "a licence never granted was never withdrawn"
    );
}

/// (h) The tripwire transition itself: an armed pool racing two sockets, the contradicting
/// arrival, and then a whole promotion cycle on the very same books.
///
/// A shard that reached primary/standby by degrading is not a shard that started there —
/// its gates are gone, its shadows carry what they delivered while the pool was armed, and
/// its markets' subscription evidence was written under a pool. This drives the topology it
/// hands back to, on that history, to the end of one promotion.
#[tokio::test]
async fn a_degraded_pool_promotes_a_standby_over_the_books_the_pool_published() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B, MARKET_C];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut books = [
        shard.observe(MARKET_A).expect("market A is in the set"),
        shard.observe(MARKET_B).expect("market B is in the set"),
        shard.observe(MARKET_C).expect("market C is in the set"),
    ];
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    for (index, slug) in markets.iter().enumerate() {
        let version = 700 + index as u64;
        send_book(&mut socket_zero, slug, "10", version).await;
        send_book(&mut socket_one, slug, "10", version).await;
    }
    for (index, slug) in markets.iter().enumerate() {
        await_book(&mut books[index], &format!("{slug} live"), is_live).await;
    }
    let armed = await_status(
        &running,
        "the standby slot's shadow to agree while the pool is still armed",
        |status| {
            status
                .connections
                .iter()
                .any(|row| row.agreeing_markets == Some(markets.len()))
        },
    )
    .await;
    assert_eq!(
        pool_state(&armed),
        Some(PoolState::Armed),
        "the shadow was maintained under an armed pool, not after it degraded"
    );

    // The venue withdraws the key the gate runs on, which is a condition the pool cannot
    // check and therefore cannot keep publishing under.
    send_keyless_book(&mut socket_zero, MARKET_A, "10").await;
    let degraded = await_status(&running, "the licence to be withdrawn", |status| {
        !matches!(pool_state(status), Some(PoolState::Armed))
    })
    .await;
    assert_eq!(
        pool_state(&degraded),
        Some(PoolState::Degraded {
            reason: PoolDegradeReason::KeyUnavailable
        }),
        "a key the gate cannot order is reported as the lost input it is"
    );

    let zero_sid = socket_zero.engine_sid();
    let one_sid = socket_one.engine_sid();
    let (mut primary, mut standby, standby_sid) =
        if role_of(&degraded, &zero_sid) == Some(ReplicaRole::PublishingPrimary) {
            (socket_zero, socket_one, one_sid)
        } else {
            (socket_one, socket_zero, zero_sid)
        };
    let standby_generation = degraded
        .connections
        .iter()
        .find(|row| row.session.as_deref() == Some(standby_sid.as_str()))
        .expect("the standby is reported")
        .generation;

    let mut bases = Vec::new();
    for (index, slug) in markets.iter().enumerate() {
        let version = 710 + index as u64;
        send_book(&mut primary, slug, "11", version).await;
        send_book(&mut standby, slug, "11", version).await;
        bases.push(
            await_book(
                &mut books[index],
                &format!("{slug} republished after the hand-back"),
                |published| levels(published, Side::Bid) == expect_levels(&[("0.51", "11")]),
            )
            .await,
        );
    }
    await_status(&running, "every shadow agreeing with its book", |status| {
        status
            .connections
            .iter()
            .any(|row| row.agreeing_markets == Some(markets.len()))
    })
    .await;

    primary.drop_abruptly().await;
    let promoted = await_status(
        &running,
        "the standby taking the publishing role",
        |status| {
            status.connections.iter().any(|row| {
                row.role == ReplicaRole::PublishingPrimary
                    && row.generation == standby_generation
                    && row.established
            })
        },
    )
    .await;
    assert_eq!(
        pool_state(&promoted),
        Some(PoolState::Degraded {
            reason: PoolDegradeReason::KeyUnavailable
        }),
        "a withdrawn licence never re-arms inside the process"
    );

    for (index, slug) in markets.iter().enumerate() {
        send_book(&mut standby, slug, "22", 720 + index as u64).await;
        let after = await_book(
            &mut books[index],
            &format!("{slug} committed by the promoted source"),
            |published| published.revision() > bases[index].revision(),
        )
        .await;
        assert!(
            is_live(&after),
            "{slug} was never stale across the promotion: {:?}",
            after.authority()
        );
        assert_eq!(
            after.revision(),
            bases[index].revision() + 1,
            "{slug}: a staleness report would have spent a revision between the two"
        );
        assert!(
            matches!(
                after.continuity(),
                MutationContinuity::Intact { epoch: 0, .. }
            ),
            "{slug}: a seamless promotion over pool-published books opens no epoch: {:?}",
            after.continuity()
        );
    }
    let status = running.status().await;
    for slug in markets {
        assert_eq!(
            status_of(&status, slug),
            Some(MarketStatus::Live),
            "{slug} is live after the promotion"
        );
    }

    let stats = running.finish().await;
    assert_eq!(
        stats.source_switches, 1,
        "the degraded topology asks the promotion question exactly as it always did"
    );
    assert_eq!(
        stats.markets_promoted,
        markets.len() as u64,
        "every market carried its own promotion evidence across"
    );
    assert_eq!(
        stats.markets_promotion_refused, 0,
        "no market was refused when every shadow agreed"
    );
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::KeyUnavailable),
        "the run names why the pool stopped publishing across sockets"
    );
}

/// (i) A known local loss on any armed socket is the book's loss.
///
/// Every socket of an armed pool is an authoritative source, so a frame this daemon
/// received and could not decode on the socket that is *not* holding the publishing role
/// still breaks the published history: the venue's counter is non-contiguous per market, so
/// what went missing on one socket need not be in any other socket's stream.
#[tokio::test]
async fn a_book_relevant_decode_failure_on_any_armed_socket_costs_the_books() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    send_book(&mut socket_zero, MARKET_A, "10", 800).await;
    let based = await_book(&mut book, "the pool to publish a base", is_live).await;

    let zero_sid = socket_zero.engine_sid();
    let status = running.status().await;
    let undecodable = "42/markets,[\"orderbookUpdate\",{\"marketSlug\":17}]";
    if role_of(&status, &zero_sid) == Some(ReplicaRole::PublishingPrimary) {
        socket_one.send_raw(undecodable).await;
    } else {
        socket_zero.send_raw(undecodable).await;
    }

    let stale = await_book(
        &mut book,
        "the book to record the local loss",
        |published| !is_live(published),
    )
    .await;
    assert!(
        !is_live(&stale),
        "an armed socket's known local loss is the book's loss: {:?}",
        stale.authority()
    );
    assert_eq!(
        pool_state(&running.status().await),
        Some(PoolState::Armed),
        "a local loss is this daemon's failure, not the venue contradicting its own key"
    );

    // The venue's observed answer to a resubscribe is the same frame again, carrying the
    // key the book already holds. A pool that treated that as nothing but a duplicate would
    // wait for a higher key a quiet market need never produce, so a redelivery the gate
    // holds exact content evidence for is installed as a recovery base instead.
    send_book(&mut socket_zero, MARKET_A, "10", 800).await;
    let rebased = await_book(&mut book, "the redelivered key to rebase the book", is_live).await;
    assert_eq!(
        levels(&rebased, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the base installed is the state that key already named"
    );
    assert!(
        rebased.continuity().epoch() > based.continuity().epoch(),
        "a recovery base is a new continuity epoch, not a diff across the gap: {:?}",
        rebased.continuity()
    );
    assert_eq!(
        pool_state(&running.status().await),
        Some(PoolState::Armed),
        "recovering from a redelivery is publishing under the licence, not against it"
    );

    let stats = running.finish().await;
    assert!(
        stats.continuity_losses >= 1,
        "the loss was reported against the book, not against a shadow"
    );
    assert_eq!(
        stats.pool_published, 2,
        "the base and the recovery base both reached the book"
    );
    assert_eq!(
        stats.pool_published_by_socket.iter().sum::<u64>(),
        stats.pool_published,
        "the per-socket credits stay a partition of what the pool published"
    );
}

/// (j) A hand-back leaves no book live behind a publishing socket that stands below it.
///
/// The licence is what let a socket other than the publishing one advance a book, and the
/// topology handed back to reads no venue key at all: the publishing connection's next frame
/// is applied as forward history whatever key it carries. So a book the publishing socket's
/// own session has not been seen to reach is a book that connection could publish backward,
/// and the hand-back is exactly where that is decided. A book its session did reach — as the
/// arrival that was published, or as the same frame arriving again — is untouched, because
/// this venue's key orders that session's own stream.
#[tokio::test]
async fn a_hand_back_stales_the_book_the_publishing_socket_stands_below() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut behind = shard.observe(MARKET_A).expect("market A is in the set");
    let mut abreast = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, mut socket_one) = connect_pool(&mut peer, &running, &markets).await;

    // A: the publishing socket publishes, and the other socket then carries the book past
    // the state that socket has ever delivered.
    send_book(&mut socket_zero, MARKET_A, "10", 100).await;
    await_book(&mut behind, "A's first base", is_live).await;
    send_book(&mut socket_one, MARKET_A, "20", 200).await;
    let ahead = await_book(
        &mut behind,
        "A to advance past the publishing socket",
        |published| levels(published, Side::Bid) == expect_levels(&[("0.51", "20")]),
    )
    .await;

    // B: both sockets deliver the same frame, so the publishing socket stands exactly at
    // what B holds.
    send_book(&mut socket_zero, MARKET_B, "10", 210).await;
    let held = await_book(&mut abreast, "B's base from the publishing socket", is_live).await;
    send_book(&mut socket_one, MARKET_B, "10", 210).await;
    await_metrics(&running, "B's second delivery to be judged", |metrics| {
        metrics.stats.pool_duplicate_drops == 1
    })
    .await;

    // The venue withdraws the key the gate runs on.
    send_keyless_book(&mut socket_one, MARKET_A, "20").await;
    let degraded = await_status(&running, "the licence to be withdrawn", |status| {
        !matches!(pool_state(status), Some(PoolState::Armed))
    })
    .await;
    assert_eq!(
        pool_state(&degraded),
        Some(PoolState::Degraded {
            reason: PoolDegradeReason::KeyUnavailable
        }),
        "a key the gate cannot order is reported as the lost input it is"
    );

    let stale = await_book(
        &mut behind,
        "A to report the authority it lost",
        |published| !is_live(published),
    )
    .await;
    assert_eq!(
        status_of(&running.status().await, MARKET_A),
        Some(MarketStatus::Stale(AuthorityReason::OrderingUnknown)),
        "the loss names the evidence that is missing, not a subscription that is not"
    );
    assert!(
        matches!(stale.continuity(), MutationContinuity::Lost { .. }),
        "the book's consumers are told the stream broke: {:?}",
        stale.continuity()
    );
    assert_eq!(
        levels(&stale, Side::Bid),
        expect_levels(&[("0.51", "20")]),
        "a lost book keeps the levels the venue last reported rather than inventing any"
    );

    tokio::time::sleep(QUIET_WINDOW).await;
    let kept = abreast.latest();
    assert!(
        is_live(&kept),
        "B stays live: the publishing socket itself delivered what B holds: {:?}",
        kept.authority()
    );
    assert_eq!(
        kept.revision(),
        held.revision(),
        "and the hand-back spends no revision on it"
    );
    assert!(
        matches!(
            kept.continuity(),
            MutationContinuity::Intact { epoch: 0, .. }
        ),
        "nor opens an epoch on it: {:?}",
        kept.continuity()
    );

    // The publishing connection's own session now delivers a frame older than what A held.
    // Under one primary that frame is ordinary forward history, so the only thing that keeps
    // it from presenting a backward step as an advance is that A is recovering: it arrives
    // as an explicit recovery base.
    send_book(&mut socket_zero, MARKET_A, "30", 150).await;
    let rebased = await_book(&mut behind, "A to take a fresh base", is_live).await;
    assert!(
        rebased.continuity().epoch() > ahead.continuity().epoch(),
        "the older frame installs a new epoch rather than splicing onto the newer state: {:?}",
        rebased.continuity()
    );
    assert_eq!(
        levels(&rebased, Side::Bid),
        expect_levels(&[("0.51", "30")]),
        "and the base installed is what that connection actually reported"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::KeyUnavailable),
        "the run names why the pool stopped publishing across sockets"
    );
    assert!(
        stats.continuity_losses >= 1,
        "the book that could have been rolled back was told it lost its authority"
    );
}

/// (k) A pool socket whose first dial died before its session began is nobody's replacement.
///
/// What the recorded conformance says about a *replacement* connection is that the venue's
/// counter continues rather than resetting, which is why a replacement's first key below the
/// published floor withdraws the licence. A socket that has never held a session makes no
/// such claim: `docs/design.md`'s own gate contract permits a socket joining a pool that is
/// already publishing to be handed a slightly earlier frame. A dial that died before the
/// venue's namespace answered hosted no session at all, so it leaves that claim unmade.
#[tokio::test]
async fn a_first_dial_that_never_established_arms_no_reconnect_rewind() {
    for _ in 0..LAYOUT_ATTEMPTS {
        let mut peer = ControlledPeer::start(patient_peer()).await;
        let markets = [MARKET_A];
        let shard = Shard::new(pool_config(peer.endpoint(), &markets))
            .expect("test shard configuration is valid");
        let mut book = shard.observe(MARKET_A).expect("market A is in the set");
        let running = Running::start(shard);

        let mut publisher = peer.next_connection().await;
        assert_eq!(
            publisher.complete_handshake().await.slugs,
            owned(&markets),
            "every pooled connection subscribes the whole desired set"
        );
        let publisher_sid = publisher.engine_sid();
        let status = await_status(&running, "the connection to take a pool socket", |status| {
            socket_of(status, &publisher_sid).is_some()
        })
        .await;
        if socket_of(&status, &publisher_sid) != Some(0) {
            // The scene needs the *redundant* socket to be the one that never establishes.
            // A publishing slot vacated before its own dial establishes hands a surviving
            // connection over instead, and a handover retires the position that connection
            // came from — which is a session ending, not the absence of one this contract is
            // about. A shard that dialled the other way round is torn down and the scene set
            // again on a fresh one, which carries nothing over.
            let _stats = running.finish().await;
            continue;
        }

        send_book(&mut publisher, MARKET_A, "10", 200).await;
        let published = await_book(&mut book, "the publishing socket's base", is_live).await;

        // The redundant socket's first dial dies before the venue's namespace answers it,
        // which is before the session it would have hosted began.
        let failed = peer.next_connection().await;
        failed.drop_abruptly().await;

        let mut joining = peer.next_connection().await;
        assert_eq!(joining.complete_handshake().await.slugs, owned(&markets));
        let joining_sid = joining.engine_sid();
        await_status(
            &running,
            "the joining socket to take the free slot",
            |status| socket_of(status, &joining_sid) == Some(1),
        )
        .await;

        // Its first key stands below the published floor, which for a first-ever session is
        // ordinary cross-connection skew and evidence of nothing.
        send_book(&mut joining, MARKET_A, "99", 190).await;
        await_metrics(&running, "the joining socket's first frame", |metrics| {
            metrics.stats.pool_stale_drops == 1
        })
        .await;
        assert_eq!(
            pool_state(&running.status().await),
            Some(PoolState::Armed),
            "a socket that never had a session cannot have rewound one"
        );

        tokio::time::sleep(QUIET_WINDOW).await;
        let after = book.latest();
        assert_eq!(
            after.revision(),
            published.revision(),
            "the earlier frame reached no book"
        );
        assert_eq!(
            levels(&after, Side::Bid),
            expect_levels(&[("0.51", "10")]),
            "the published book still holds the newer state"
        );

        // The pool goes on publishing across both sockets from there.
        send_book(&mut joining, MARKET_A, "40", 210).await;
        await_book(&mut book, "the joined socket to publish", |published| {
            levels(published, Side::Bid) == expect_levels(&[("0.51", "40")])
        })
        .await;

        let stats = running.finish().await;
        assert_eq!(
            stats.pool_degraded, None,
            "no observation contradicted the recorded basis"
        );
        assert_eq!(
            stats.pool_stale_drops, 1,
            "the joining socket's earlier frame was dropped as the skew it is"
        );
        return;
    }
    panic!("no shard of {LAYOUT_ATTEMPTS} established its publishing socket first");
}

/// (l) A dial from before a market was re-added publishes nothing into its new incarnation.
///
/// A removed market's book is retired and a market that comes back comes back as a fresh
/// incarnation with its own empty gate. A connection dialled before the removal still names
/// that slug in the set it put on the wire, and its dialled set is immutable, so carriage
/// read from that set alone would let a frame belonging to the retired subscription land in
/// the new incarnation as its base — a book rebased onto a frame from a set the daemon has
/// abandoned. Carriage is therefore read against the incarnation the entry actually is.
#[tokio::test]
async fn a_dial_from_before_a_re_add_publishes_nothing_into_the_new_incarnation() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let running = Running::start(shard);
    let (mut socket_zero, socket_one) = connect_pool(&mut peer, &running, &markets).await;

    let mut retiring = running
        .handle
        .observe(MARKET_A)
        .await
        .expect("the shard is running")
        .expect("market A is in the set");
    send_book(&mut socket_zero, MARKET_A, "10", 200).await;
    await_book(&mut retiring, "A's base under the pool", is_live).await;

    // The redundant socket is replaced, and its replacement's dial — which carries A — has
    // not yet been answered by the venue's namespace, so nothing can reconcile it.
    socket_one.drop_abruptly().await;
    let mut in_flight = peer.next_connection().await;

    // A is removed and added back: its book is retired and a fresh incarnation installed.
    let removed = running
        .handle
        .remove(owned(&[MARKET_A]))
        .await
        .expect("the shard is running");
    assert_eq!(
        removed.first().map(|outcome| outcome.status.clone()),
        Some(MarketStatus::Removed),
        "A left the desired set"
    );
    let mut without_a = peer.next_connection().await;
    assert_eq!(
        without_a.complete_handshake().await.slugs,
        owned(&[MARKET_B]),
        "the set change is carried on a fresh connection generation"
    );
    let added = running
        .handle
        .add(owned(&[MARKET_A]))
        .await
        .expect("the shard is running");
    assert_eq!(
        added.first().map(|outcome| outcome.status.clone()),
        Some(MarketStatus::Accepted),
        "A came back into the desired set"
    );
    let mut rebuilt = running
        .handle
        .observe(MARKET_A)
        .await
        .expect("the shard is running")
        .expect("market A is in the set again");
    assert!(
        !is_live(&rebuilt.latest()),
        "the new incarnation starts owed a base"
    );

    // The pre-removal dial is answered only now, and the venue serves it the set it asked
    // for — the one that still names A.
    assert_eq!(
        in_flight.complete_handshake().await.slugs,
        owned(&markets),
        "the in-flight dial carries the set it was given before the removal"
    );
    send_book(&mut in_flight, MARKET_A, "77", 150).await;
    tokio::time::sleep(QUIET_WINDOW).await;

    let untouched = rebuilt.latest();
    assert!(
        !is_live(&untouched),
        "a frame from the retired subscription is no base for the new incarnation: {:?}",
        untouched.authority()
    );
    assert!(
        levels(&untouched, Side::Bid).is_empty(),
        "and reaches the book's levels not at all: {:?}",
        levels(&untouched, Side::Bid)
    );
    assert_eq!(
        status_of(&running.status().await, MARKET_A),
        Some(MarketStatus::Reconciling),
        "A is still waiting for a generation that was dialled with it"
    );

    // A generation dialled with the market as it now stands publishes it.
    let mut carrier = None;
    for _ in 0..MAX_POOL_SOCKETS {
        let mut connection = peer.next_connection().await;
        if connection.complete_handshake().await.slugs == owned(&markets) {
            carrier = Some(connection);
            break;
        }
    }
    let mut carrier = carrier.expect("a connection is dialled with the desired set");
    send_book(&mut carrier, MARKET_A, "20", 210).await;
    let based = await_book(
        &mut rebuilt,
        "A to take its base from a current dial",
        is_live,
    )
    .await;
    assert_eq!(
        levels(&based, Side::Bid),
        expect_levels(&[("0.51", "20")]),
        "the base A holds is the one a current generation reported"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded, None,
        "nothing here contradicted the recorded basis"
    );
}

/// (m) A pool socket that hosted a session and ended is a replacement for every market,
/// including the ones that were quiet while it ran.
///
/// A market's gate is created by the first arrival that reaches it, so a session that began
/// and ended while a market was quiet leaves no gate to have recorded it. The venue's counter
/// is recorded continuing across a reconnect, and that claim is what a replacement's first
/// key below the published floor contradicts — a fact about the socket, not about which
/// market happened to be busy. A gate built late must therefore judge that socket exactly as
/// one that existed all along would.
#[tokio::test]
async fn a_session_that_ended_while_a_market_was_quiet_still_makes_its_socket_a_replacement() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(pool_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut socket_zero, socket_one) = connect_pool(&mut peer, &running, &markets).await;

    // The redundant socket hosts a whole session and ends, with the venue never having said
    // anything about the one market in the set.
    socket_one.drop_abruptly().await;
    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&markets),
        "the replacement rejoins the pool on the whole desired set"
    );
    let replacement_sid = replacement.engine_sid();
    await_status(
        &running,
        "the replacement to take the free socket",
        |status| socket_of(status, &replacement_sid) == Some(1),
    )
    .await;

    // Only now does the venue speak about the market, which is when its gate comes into
    // being.
    send_book(&mut socket_zero, MARKET_A, "10", 200).await;
    let published = await_book(&mut book, "the market's first base", is_live).await;

    // The replacement's first key stands below that floor: the venue's counter did not
    // continue across the reconnect, which is the recorded basis being contradicted.
    send_book(&mut replacement, MARKET_A, "99", 190).await;
    let degraded = await_status(&running, "the tripwire to withdraw the licence", |status| {
        !matches!(pool_state(status), Some(PoolState::Armed))
    })
    .await;
    assert_eq!(
        pool_state(&degraded),
        Some(PoolState::Degraded {
            reason: PoolDegradeReason::ReconnectRewind
        }),
        "the withdrawal names the observation that caused it"
    );

    tokio::time::sleep(QUIET_WINDOW).await;
    let after = book.latest();
    assert_eq!(
        after.revision(),
        published.revision(),
        "the arrival that withdrew the licence reached no book"
    );
    assert_eq!(
        levels(&after, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the book still holds the state the pool published"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.pool_degraded,
        Some(PoolDegradeReason::ReconnectRewind),
        "the run names why the pool stopped publishing across sockets"
    );
    assert_eq!(
        stats.pool_stale_drops, 0,
        "a replacement's rewind is a violation of the basis, never ordinary skew"
    );
}
