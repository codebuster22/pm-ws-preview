#![forbid(unsafe_code)]

//! Multi-market shard contracts for the real `limitless::shard` and `limitless::connection`
//! code, driven against the scripted controlled peer.
//!
//! What is proven here is what an operator and a consumer would see: subscription payloads
//! are read off the wire by the peer rather than from a daemon-side counter, and book state
//! is read through a [`BookObserver`] attached to the market's own writer. Nothing is
//! synchronized by sleeping and hoping — every step waits on the peer's script, on a book
//! transition, or on the configured command pacing floor.

mod support;

use pm_ws::limitless::shard::{
    MarketRejection, MarketStatus, Shard, ShardConfig, ShardHandle, ShardSegment, ShardStats,
    ShardStatus, ShardStopper, SubscriptionState,
};
use pm_ws::limitless::supervisor::VENUE;
use pm_ws::{
    AuthorityReason, AuthorityState, BookObserver, BookSnapshot, ContinuityReason, EventPoll,
    EventStream, MarketHandle, MarketRef, MutationContinuity, MutationCursor, NativeIdentifierKind,
    NativeMarketKey, PublishedBook, RetainedEvent, SegmentConfig, SegmentLayout, SegmentReader,
    SegmentRegion, SegmentWriter, Side, StreamFault, Venue, WriterError,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const MARKET_A: &str = "btc-up-or-down-5-min-1788172500";
const MARKET_B: &str = "eth-up-or-down-5-min-1788172500";
const MARKET_C: &str = "sol-up-or-down-5-min-1788172500";
const UNKNOWN_MARKET: &str = "xrp-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
/// Longer than the daemon's default 500 ms command floor, so a reissue that was going to be
/// emitted has certainly been emitted by the time a negative assertion gives up waiting for it.
const NO_COMMAND_WINDOW: Duration = Duration::from_millis(900);
/// The configured command floor, less the scheduling slack between the instant a command is
/// authorized and the instant its bytes reach the peer.
const PACING_FLOOR: Duration = Duration::from_millis(450);
const QUIET_PING_INTERVAL: Duration = Duration::from_millis(400);
const QUIET_PING_CYCLES: usize = 6;
/// Short enough that a venue which never answers a reissue is given up on inside a test
/// step, and far longer than the local write it is measuring.
const SWALLOWED_REISSUE_WINDOW: Duration = Duration::from_millis(300);
const STALL: Duration = Duration::from_millis(300);
const STALL_BURST: u64 = 20;
/// The smallest ingest queue that can carry a connection's establishment notices — the
/// venue-negotiated open, then the subscription acknowledgment — before the first book frame
/// arrives. A shorter queue drops early frames while those markers wait for room, which is
/// ordinary bounded-queue behavior and not what an overload test is about.
const ESTABLISHMENT_INGEST_FLOOR: usize = 2;
const OVERLOAD_BURST: u64 = 10;
const OVERLOAD_ROUNDS: u64 = 20;
const OVERLOAD_ROUND_INTERVAL: Duration = Duration::from_millis(2);
const SCALE_MARKETS: usize = 300;
/// A ring deep enough that no test here laps it, and a power of two so a position masks to a
/// slot.
const SEGMENT_EVENTS: u32 = 64;
const SEGMENT_INSTANCE: u128 = 0x2026_0902_0000_0001_0000_0000_0000_0001;
const SEGMENT_POLL: Duration = Duration::from_millis(1);
/// The venue's own resolution timestamp shape, as `docs/limitless.md` records it.
const RESOLUTION_DATE: &str = "2026-09-01T15:05:00.000Z";

/// A peer whose announced heartbeat cadence is far longer than any test step, so a test
/// about something other than liveness cannot be disturbed by a heartbeat deadline.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// Production policy with the reconnect clock compressed: the same code paths, spaced so a
/// test finishes in seconds. `resubscribe_window` is deliberately far longer than any step,
/// so only a test that scripts a swallowed reissue ever sees the window expire.
fn test_config(endpoint: String, markets: &[&str]) -> ShardConfig {
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

    async fn observe(&self, slug: &str) -> Option<BookObserver> {
        self.handle
            .observe(slug)
            .await
            .expect("the shard is running")
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

fn is_stale(published: &PublishedBook, reason: AuthorityReason) -> bool {
    published.authority() == &AuthorityState::Stale(reason)
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

fn subscription_of(status: &ShardStatus, slug: &str) -> Option<SubscriptionState> {
    status
        .markets
        .iter()
        .find(|report| report.slug == slug)
        .map(|report| report.subscription)
}

fn drain(observer: &mut BookObserver) {
    while let Ok(Some(_)) = observer.try_recv() {}
}

/// Sends one distinguishable book for `slug`, sized so each market's levels are unique.
async fn send_book(connection: &mut PeerConnection, slug: &str, size: &str, version: u64) {
    connection
        .send_orderbook(slug, &[("0.51", size)], &[("0.52", size)], Some(version))
        .await;
}

/// Applies one more accepted frame to `slug` and waits for it, which proves the shard has
/// drained everything the peer sent before it. Notices are delivered in order, so a frame
/// applied here is evidence that every earlier notice was applied first.
async fn fence(
    connection: &mut PeerConnection,
    observer: &mut BookObserver,
    slug: &str,
    size: &str,
    version: u64,
) -> Arc<PublishedBook> {
    let before = observer.latest().revision();
    send_book(connection, slug, size, version).await;
    await_book(observer, "the fencing frame to be applied", |published| {
        published.revision() > before
    })
    .await
}

#[tokio::test]
async fn a_shard_subscribes_its_whole_set_and_routes_every_frame_to_its_own_book() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(
        peer.endpoint(),
        &[MARKET_C, MARKET_A, MARKET_B],
    ))
    .expect("test shard configuration is valid");
    let mut books = [
        shard.observe(MARKET_A).expect("market A is in the set"),
        shard.observe(MARKET_B).expect("market B is in the set"),
        shard.observe(MARKET_C).expect("market C is in the set"),
    ];
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(
        request.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "one command carries the whole set, in a stable order"
    );

    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    connection
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;
    connection
        .send_orderbook(MARKET_C, &[("0.71", "30")], &[("0.72", "31")], Some(3))
        .await;

    let a = await_book(&mut books[0], "market A live", is_live).await;
    let b = await_book(&mut books[1], "market B live", is_live).await;
    let c = await_book(&mut books[2], "market C live", is_live).await;
    assert_eq!(
        levels(&a, Side::Bid),
        vec![("0.51".to_owned(), "10".to_owned())]
    );
    assert_eq!(
        levels(&a, Side::Ask),
        vec![("0.52".to_owned(), "11".to_owned())]
    );
    assert_eq!(
        levels(&b, Side::Bid),
        vec![("0.31".to_owned(), "20".to_owned())]
    );
    assert_eq!(
        levels(&c, Side::Bid),
        vec![("0.71".to_owned(), "30".to_owned())]
    );

    let status = running.status().await;
    assert_eq!(status.desired, 3);
    assert!(status.subscribed);
    for slug in [MARKET_A, MARKET_B, MARKET_C] {
        assert_eq!(status_of(&status, slug), Some(MarketStatus::Live));
        assert_eq!(
            subscription_of(&status, slug),
            Some(SubscriptionState::Established)
        );
    }

    let stats = running.finish().await;
    assert_eq!(stats.snapshots_applied, 3);
    assert_eq!(stats.frames_unrouted, 0);
    assert_eq!(stats.subscriptions_emitted, 1);
}

/// A set change is carried by a fresh connection generation, which is the only attribution
/// this venue's evidence supports.
///
/// The retained market pays the honest price rather than an invented one: an explicit
/// reconnect continuity loss and a fresh base on the connection that carries the new set. No
/// frame is attributed to a set it might not belong to, and the added market cannot be
/// populated by anything the old connection sent.
#[tokio::test]
async fn an_added_market_is_carried_by_a_fresh_connection_and_the_retained_market_recovers() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    send_book(&mut connection, MARKET_A, "10", 1).await;
    let before = await_book(&mut a, "market A live", is_live).await;

    let accepted = running
        .handle
        .add(owned(&[MARKET_C]))
        .await
        .expect("the shard is running");
    assert_eq!(accepted[0].status, MarketStatus::Accepted);

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "the new set is dialled whole on a fresh generation, never patched onto the old one"
    );
    await_book(
        &mut a,
        "market A stale across the replacement",
        |published| is_stale(published, AuthorityReason::SubscriptionLost),
    )
    .await;

    send_book(&mut replacement, MARKET_A, "11", 2).await;
    send_book(&mut replacement, MARKET_C, "30", 3).await;
    let recovered = await_book(&mut a, "market A live on the replacement", is_live).await;
    assert!(
        recovered.continuity().epoch() > before.continuity().epoch(),
        "a market carried across a replacement recovers on a fresh base with the gap declared"
    );
    let mut c = running
        .observe(MARKET_C)
        .await
        .expect("market C has a book once it is in the set");
    await_book(
        &mut c,
        "market C live on the connection carrying it",
        is_live,
    )
    .await;

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.set_replacements, 1);
    assert_eq!(
        stats.subscriptions_emitted, 2,
        "one establishing subscription per connection, and no same-connection reissue"
    );
}

/// The recovery base a retained market gets on the replacement connection declares the gap
/// instead of papering over it.
///
/// The venue re-sends exactly the book the market already had. That is not evidence the
/// stream was continuous — nothing was observed across the replacement — so the base opens a
/// new continuity epoch and derives no mutations at all, rather than a diff that would claim
/// the depth had not moved.
#[tokio::test]
async fn a_retained_market_rebases_across_a_replacement_without_deriving_diffs_over_the_gap() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_A, "11", 2).await;
    await_book(&mut a, "market A at its second book", |published| {
        published.revision() >= 2
    })
    .await;
    drain(&mut a);
    let before = a.latest();

    running
        .handle
        .add(owned(&[MARKET_C]))
        .await
        .expect("the shard is running");
    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C])
    );
    await_book(
        &mut a,
        "market A stale across the replacement",
        |published| !is_live(published),
    )
    .await;
    drain(&mut a);

    send_book(&mut replacement, MARKET_A, "11", 3).await;
    let after = await_book(&mut a, "market A live on the replacement", is_live).await;
    assert_eq!(
        after.continuity().epoch(),
        before.continuity().epoch() + 1,
        "an unobserved interval is a continuity break, whatever the book looks like after it"
    );
    assert_eq!(levels(&after, Side::Bid), levels(&before, Side::Bid));
    assert!(
        matches!(a.try_recv(), Ok(None)),
        "a recovery base derives no mutation across the gap it declares"
    );

    let stats = running.finish().await;
    assert_eq!(stats.continuity_losses, 1);
    assert_eq!(stats.connection_attempts, 2);
}

/// A removal is reconciled by the connection ending, which is when the venue stops sending.
#[tokio::test]
async fn a_removed_market_is_dropped_when_its_connection_ends_and_the_replacement_carries_the_rest()
{
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(
        peer.endpoint(),
        &[MARKET_A, MARKET_B, MARKET_C],
    ))
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    send_book(&mut connection, MARKET_C, "30", 3).await;
    await_book(&mut a, "market A live", is_live).await;

    let removed = running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(removed[0].status, MarketStatus::Removed);

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_C]),
        "the remaining set is dialled whole, never an unsubscribe of one market"
    );
    send_book(&mut replacement, MARKET_A, "11", 4).await;
    await_book(&mut a, "market A live on the replacement", is_live).await;

    let status = running.status().await;
    assert_eq!(status.desired, 2);
    assert_eq!(
        status_of(&status, MARKET_B),
        None,
        "the removed market's book went with the connection that carried it"
    );

    let stats = running.finish().await;
    assert_eq!(stats.markets_dropped, 1);
    assert_eq!(stats.set_replacements, 1);
}

/// A removed market keeps its book for exactly as long as the venue keeps sending it — until
/// the connection carrying the old set ends — and comes back as a fresh incarnation.
#[tokio::test]
async fn a_removed_market_is_unsubscribed_with_its_connection_and_comes_back_fresh() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;

    running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    await_book(&mut b, "market B unsubscribed", |published| {
        matches!(published.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert!(
        running.observe(MARKET_B).await.is_none(),
        "the book is gone once the connection carrying it has ended"
    );

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    send_book(&mut replacement, MARKET_A, "11", 3).await;
    await_book(&mut a, "market A live on the replacement", is_live).await;

    running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let mut rejoined = peer.next_connection().await;
    assert_eq!(
        rejoined.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    send_book(&mut rejoined, MARKET_B, "24", 4).await;
    let mut back = running
        .observe(MARKET_B)
        .await
        .expect("market B has a book again");
    let published = await_book(&mut back, "the re-added market's first base", is_live).await;
    assert_eq!(
        published.revision(),
        1,
        "a re-added market gets a fresh book, never the retained one"
    );
    assert_eq!(
        levels(&published, Side::Bid),
        vec![("0.51".to_owned(), "24".to_owned())]
    );

    let stats = running.finish().await;
    assert_eq!(stats.markets_dropped, 1);
    assert_eq!(stats.set_replacements, 2);
}

#[tokio::test]
async fn an_undecodable_book_frame_stales_the_established_markets_and_one_reissue_recovers_them() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(
        peer.endpoint(),
        &[MARKET_A, MARKET_B, MARKET_C],
    ))
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let c = shard.observe(MARKET_C).expect("market C is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;

    connection
        .send_orderbook(MARKET_A, &[("2", "100")], &[("0.6", "200")], Some(3))
        .await;
    await_book(&mut a, "market A Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    await_book(&mut b, "market B Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    assert!(
        matches!(c.latest().authority(), AuthorityState::Synchronizing),
        "a market that never established has no authority to lose to the rail's failure"
    );

    assert_eq!(
        connection.expect_resubscription(STEP_TIMEOUT).await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "recovery asks the healthy connection for the whole set, never one market"
    );
    send_book(&mut connection, MARKET_A, "13", 4).await;
    send_book(&mut connection, MARKET_B, "23", 5).await;
    let recovered_a = await_book(&mut a, "market A recovered", is_live).await;
    let recovered_b = await_book(&mut b, "market B recovered", is_live).await;
    assert_eq!(recovered_a.continuity().epoch(), 1);
    assert_eq!(recovered_b.continuity().epoch(), 1);

    let stats = running.finish().await;
    assert_eq!(
        stats.connection_attempts, 1,
        "resubscribe comes before reconnect"
    );
    assert_eq!(stats.reissues_escalated, 0);
    assert_eq!(stats.subscriptions_emitted, 2);
    assert_eq!(stats.continuity_losses, 2);
    assert!(!stats.decode_failures.is_empty());
}

#[tokio::test]
async fn a_reissue_the_venue_never_answers_escalates_to_a_replacement_connection() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        resubscribe_window: SWALLOWED_REISSUE_WINDOW,
        ..test_config(peer.endpoint(), &[MARKET_A, MARKET_B])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;

    connection
        .send_orderbook(MARKET_A, &[("2", "100")], &[("0.6", "200")], Some(3))
        .await;
    await_book(&mut a, "market A Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    assert_eq!(
        connection.expect_resubscription(STEP_TIMEOUT).await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the reissue reaches the wire before the venue is given its window"
    );

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "a venue that never answered the reissue is replaced, carrying the whole set"
    );
    send_book(&mut replacement, MARKET_A, "13", 4).await;
    send_book(&mut replacement, MARKET_B, "23", 5).await;
    let recovered_a = await_book(&mut a, "market A recovered", is_live).await;
    let recovered_b = await_book(&mut b, "market B recovered", is_live).await;
    assert!(recovered_a.continuity().epoch() >= 1);
    assert!(recovered_b.continuity().epoch() >= 1);

    let stats = running.finish().await;
    assert_eq!(stats.reissues_escalated, 1);
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.fenced_generations, 1);
}

#[tokio::test]
async fn a_rejected_candidate_costs_only_the_market_it_names() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        level_capacity: 2,
        ..test_config(peer.endpoint(), &[MARKET_A, MARKET_B])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;
    drain(&mut b);
    let untouched = b.latest();

    connection
        .send_orderbook(
            MARKET_A,
            &[("0.52", "10"), ("0.51", "10")],
            &[("0.53", "10")],
            Some(3),
        )
        .await;
    await_book(&mut a, "market A Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    let after = b.latest();
    assert!(
        is_live(&after),
        "a candidate this daemon refused for one market is not another market's loss"
    );
    assert_eq!(after.revision(), untouched.revision());
    assert!(matches!(b.try_recv(), Ok(None)));

    assert_eq!(
        connection.expect_resubscription(STEP_TIMEOUT).await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    send_book(&mut connection, MARKET_A, "13", 4).await;
    let recovered = await_book(&mut a, "the recovery base on the same connection", is_live).await;
    assert_eq!(recovered.continuity().epoch(), 1);

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 1);
    assert_eq!(stats.continuity_losses, 1);
    assert_eq!(
        stats.decode_failures.get("book:InvalidCandidate").copied(),
        Some(1)
    );
}

#[tokio::test]
async fn repeating_an_add_or_a_remove_puts_no_command_on_the_wire() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;

    let repeated = running
        .handle
        .add(owned(&[MARKET_A]))
        .await
        .expect("the shard is running");
    assert_eq!(
        repeated[0].status,
        MarketStatus::Live,
        "a repeated add answers the market's current state, not a fresh acceptance"
    );
    let absent = running
        .handle
        .remove(owned(&[MARKET_C]))
        .await
        .expect("the shard is running");
    assert_eq!(absent[0].status, MarketStatus::Removed);

    connection.expect_no_subscription(NO_COMMAND_WINDOW).await;

    let stats = running.finish().await;
    assert_eq!(
        stats.subscriptions_emitted, 1,
        "only the establishing subscription reached the venue"
    );
    assert_eq!(stats.connection_attempts, 1);
}

/// Two set changes in a row are two dials, and the venue sees their establishing
/// subscriptions no closer than the configured sustained-command floor.
#[tokio::test]
async fn two_set_changes_never_command_the_venue_inside_the_configured_floor() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let running = Running::start(shard);

    let mut first = peer.next_connection().await;
    first.complete_namespace().await;
    assert_eq!(
        first
            .expect_unacknowledged_resubscription(STEP_TIMEOUT)
            .await
            .slugs,
        owned(&[MARKET_A])
    );

    running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let mut second = peer.next_connection().await;
    second.complete_namespace().await;
    assert_eq!(
        second
            .expect_unacknowledged_resubscription(STEP_TIMEOUT)
            .await
            .slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    let after_second = Instant::now();

    running
        .handle
        .add(owned(&[MARKET_C]))
        .await
        .expect("the shard is running");
    let mut third = peer.next_connection().await;
    third.complete_namespace().await;
    assert_eq!(
        third
            .expect_unacknowledged_resubscription(STEP_TIMEOUT)
            .await
            .slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C])
    );
    let gap = Instant::now().saturating_duration_since(after_second);
    assert!(
        gap >= PACING_FLOOR,
        "two subscription frames landed {gap:?} apart on the wire, inside the configured floor"
    );

    let stats = running.finish().await;
    assert_eq!(stats.subscriptions_emitted, 3);
    assert_eq!(stats.set_replacements, 2);
}

#[tokio::test]
async fn a_frame_for_a_market_outside_the_set_is_counted_and_dropped() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;

    send_book(&mut connection, UNKNOWN_MARKET, "99", 2).await;
    let fenced = fence(&mut connection, &mut a, MARKET_A, "11", 3).await;
    assert!(is_live(&fenced), "an unknown market is never fatal");
    assert!(running.observe(UNKNOWN_MARKET).await.is_none());

    let status = running.status().await;
    assert_eq!(status.markets.len(), 1, "no book was created for it");

    let stats = running.finish().await;
    assert_eq!(stats.frames_unrouted, 1);
    assert_eq!(stats.continuity_losses, 0);
    assert!(stats.decode_failures.is_empty());
}

#[tokio::test]
async fn queue_age_grows_under_an_ingest_stall_and_falls_once_the_backlog_drains() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        ingest_stall: Some(STALL),
        ..test_config(peer.endpoint(), &[MARKET_A])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "the first accepted base", is_live).await;

    for version in 0..STALL_BURST {
        send_book(&mut connection, MARKET_A, "11", version + 2).await;
    }
    await_book(&mut a, "the whole stalled burst to drain", |published| {
        published.revision() > STALL_BURST
    })
    .await;

    let backlogged = running.status().await.queue_age;
    let stall_micros = u64::try_from(STALL.as_micros()).expect("the stall fits a u64");
    assert!(
        backlogged.max_micros >= stall_micros,
        "the withheld drain cycle must show as queue age, got {backlogged:?}"
    );
    assert!(backlogged.p99_micros <= backlogged.max_micros);
    assert!(backlogged.samples >= STALL_BURST);

    fence(&mut connection, &mut a, MARKET_A, "12", STALL_BURST + 2).await;
    let drained = running.status().await.queue_age;
    assert_eq!(drained.max_micros, backlogged.max_micros);
    assert!(
        drained.last_micros * 2 < drained.max_micros,
        "a frame arriving on a drained queue is far younger than the backlog's worst, got {drained:?}"
    );

    let stats = running.finish().await;
    assert!(
        stats.queue_depth_max >= 2,
        "the ingest queue actually held a backlog"
    );
    assert_eq!(stats.overload_drops, 0, "the burst fits the bounded queue");
    assert_eq!(stats.queue_age.max_micros, backlogged.max_micros);
}

#[tokio::test]
async fn an_overloaded_shard_queue_stales_only_the_markets_that_held_authority() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        ingest_capacity: ESTABLISHMENT_INGEST_FLOOR,
        ..test_config(peer.endpoint(), &[MARKET_A, MARKET_B])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "the first accepted base", is_live).await;

    let mut version = 2;
    for _ in 0..OVERLOAD_ROUNDS {
        for _ in 0..OVERLOAD_BURST {
            send_book(&mut connection, MARKET_A, "11", version).await;
            version += 1;
        }
        tokio::time::sleep(OVERLOAD_ROUND_INTERVAL).await;
    }
    await_book(
        &mut a,
        "Stale(Overload) from the overflowing ingest queue",
        |published| is_stale(published, AuthorityReason::Overload),
    )
    .await;
    assert!(
        matches!(b.latest().authority(), AuthorityState::Synchronizing),
        "a market that never established has no authority to lose to another market's overload"
    );

    let stats = running.finish().await;
    assert!(
        stats.overload_drops > 0,
        "the capacity-one ingest queue must actually have overflowed"
    );
    assert!(stats.continuity_losses > 0);
    assert_eq!(
        stats.connection_attempts, 1,
        "an overloaded queue is recovered on the connection that overflowed it"
    );
}

#[tokio::test]
async fn a_shard_reconnect_resubscribes_the_whole_set_and_recovers_each_market_on_evidence() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(
        peer.endpoint(),
        &[MARKET_A, MARKET_B, MARKET_C],
    ))
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let c = shard.observe(MARKET_C).expect("market C is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;
    assert!(
        matches!(c.latest().authority(), AuthorityState::Synchronizing),
        "market C is quiet: the venue has said nothing about it"
    );

    connection.drop_abruptly().await;
    await_book(&mut a, "market A stale on the lost rail", |published| {
        is_stale(published, AuthorityReason::Disconnect)
    })
    .await;
    await_book(&mut b, "market B stale on the lost rail", |published| {
        is_stale(published, AuthorityReason::Disconnect)
    })
    .await;
    assert!(
        matches!(c.latest().authority(), AuthorityState::Synchronizing),
        "a market that never established reports no loss when the rail dies"
    );

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "a replacement connection carries the whole desired set"
    );
    send_book(&mut replacement, MARKET_A, "12", 3).await;
    send_book(&mut replacement, MARKET_B, "22", 4).await;
    let recovered_a = await_book(&mut a, "market A recovered", is_live).await;
    let recovered_b = await_book(&mut b, "market B recovered", is_live).await;
    assert_eq!(
        recovered_a.continuity().epoch(),
        1,
        "the first snapshot after the loss is a recovery base"
    );
    assert_eq!(recovered_b.continuity().epoch(), 1);
    assert!(
        matches!(c.latest().authority(), AuthorityState::Synchronizing),
        "a quiet market is still simply waiting, never stale"
    );

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.subscriptions_emitted, 2);
    assert_eq!(stats.continuity_losses, 2);
}

#[tokio::test]
async fn a_quiet_market_stays_live_while_the_rest_of_the_set_is_busy() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;
    let quiet = b.latest();

    for version in (3..).take(QUIET_PING_CYCLES) {
        connection.send_ping().await;
        connection.expect_pong(STEP_TIMEOUT).await;
        fence(&mut connection, &mut a, MARKET_A, "11", version).await;
        tokio::time::sleep(QUIET_PING_INTERVAL).await;
    }

    let still = b.latest();
    assert!(
        is_live(&still),
        "a quiet market on a healthy connection is never staled by silence"
    );
    assert_eq!(still.revision(), quiet.revision());
    assert!(a.latest().revision() > quiet.revision());

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 1);
    assert_eq!(stats.continuity_losses, 0);
}

#[tokio::test]
async fn a_desired_set_of_three_hundred_markets_subscribes_and_routes_without_scanning_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let slugs: Vec<String> = (0..SCALE_MARKETS)
        .map(|index| format!("scale-market-{index:04}"))
        .collect();
    let shard = Shard::new(ShardConfig {
        markets: slugs.clone(),
        observer_capacity: 8,
        level_capacity: 64,
        ..test_config(peer.endpoint(), &[])
    })
    .expect("test shard configuration is valid");
    let mut first = shard
        .observe(&slugs[0])
        .expect("the first market is in the set");
    let mut last = shard
        .observe(&slugs[SCALE_MARKETS - 1])
        .expect("the last market is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(request.slugs.len(), SCALE_MARKETS);
    assert_eq!(request.slugs, {
        let mut sorted = slugs.clone();
        sorted.sort();
        sorted
    });

    send_book(&mut connection, &slugs[0], "10", 1).await;
    send_book(&mut connection, &slugs[SCALE_MARKETS - 1], "20", 2).await;
    let opened = await_book(&mut first, "the first market live", is_live).await;
    let closed = await_book(&mut last, "the last market live", is_live).await;
    assert_eq!(
        levels(&opened, Side::Bid),
        vec![("0.51".to_owned(), "10".to_owned())]
    );
    assert_eq!(
        levels(&closed, Side::Bid),
        vec![("0.51".to_owned(), "20".to_owned())]
    );

    let status = running.status().await;
    assert_eq!(status.markets.len(), SCALE_MARKETS);
    assert_eq!(status.desired, SCALE_MARKETS);
    assert_eq!(
        status_of(&status, &slugs[1]),
        Some(MarketStatus::Reconciling),
        "a market the venue has not spoken about is reconciling, never live and never stale"
    );

    let stats = running.finish().await;
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(stats.frames_unrouted, 0);
    assert_eq!(stats.subscriptions_emitted, 1);
}

#[tokio::test]
async fn removing_an_invalid_identifier_is_rejected_input_rather_than_a_no_op() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;

    let answered = running
        .handle
        .remove(vec![String::new()])
        .await
        .expect("the shard is running");
    assert_eq!(
        answered[0].status,
        MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
        "a slug that is not an identifier is rejected input, not an absent market"
    );
    connection.expect_no_subscription(NO_COMMAND_WINDOW).await;

    let stats = running.finish().await;
    assert_eq!(stats.subscriptions_emitted, 1);
}

#[tokio::test]
async fn an_emptied_desired_set_gives_up_the_connection_and_a_new_market_dials_again() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;

    running
        .handle
        .remove(owned(&[MARKET_A]))
        .await
        .expect("the shard is running");
    assert!(
        connection.read_text_frame(STEP_TIMEOUT).await.is_none(),
        "an emptied set closes the socket instead of emitting a subscription naming nothing"
    );
    let ended = await_book(&mut a, "market A unsubscribed", |published| {
        matches!(published.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert!(
        ended.revision() > 1,
        "a consumer holding only latest state is told the book it holds is over"
    );
    let emptied = running.status().await;
    assert_eq!(emptied.desired, 0);
    assert!(emptied.markets.is_empty(), "no book is retained for nobody");
    assert!(!emptied.subscribed);

    running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let mut rejoined = peer.next_connection().await;
    assert_eq!(
        rejoined.complete_handshake().await.slugs,
        owned(&[MARKET_B]),
        "a market added to an empty set dials a fresh connection carrying it"
    );
    send_book(&mut rejoined, MARKET_B, "20", 2).await;
    let mut b = running
        .observe(MARKET_B)
        .await
        .expect("market B has a book once it is in the set");
    await_book(&mut b, "market B live on the new connection", is_live).await;

    let stats = running.finish().await;
    assert_eq!(stats.connections_shed, 1);
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.subscriptions_emitted, 2);
    assert_eq!(stats.markets_dropped, 1);
}

#[tokio::test]
async fn a_shard_with_no_markets_holds_no_connection_until_one_is_added() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let running = Running::start(
        Shard::new(test_config(peer.endpoint(), &[])).expect("test shard configuration is valid"),
    );

    assert!(
        !peer.has_pending_connection().await,
        "a shard wanting nothing dials nothing"
    );
    let empty = running.status().await;
    assert_eq!(empty.desired, 0);
    assert!(!empty.subscribed);

    running
        .handle
        .add(owned(&[MARKET_A]))
        .await
        .expect("the shard is running");
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    send_book(&mut connection, MARKET_A, "10", 1).await;
    let mut a = running
        .observe(MARKET_A)
        .await
        .expect("market A has a book once it is in the set");
    await_book(&mut a, "market A live", is_live).await;

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 1);
    assert_eq!(stats.connections_shed, 0);
}

/// A removal still in flight when the connection dies is reconciled by the death, and every
/// frame the fenced generation produced afterwards reaches nothing.
///
/// This is the whole of the attribution argument in one run: the old set's frames cannot
/// touch the new set's books, because the notices carrying them name a generation this shard
/// no longer runs. Nothing has to reason about which command the venue had processed.
#[tokio::test]
async fn frames_from_the_fenced_connection_after_a_set_change_reach_no_book() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;

    running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    await_book(
        &mut b,
        "market B unsubscribed with its connection",
        |published| matches!(published.authority(), AuthorityState::Unsubscribed),
    )
    .await;

    send_book(&mut connection, MARKET_B, "21", 3).await;
    send_book(&mut connection, MARKET_A, "99", 4).await;
    connection.drop_abruptly().await;

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    send_book(&mut replacement, MARKET_A, "11", 5).await;
    let published = await_book(&mut a, "market A live on the replacement", is_live).await;
    assert_eq!(
        levels(&published, Side::Bid),
        vec![("0.51".to_owned(), "11".to_owned())],
        "the replacement's own base is what market A holds, not the fenced generation's frame"
    );
    assert!(running.observe(MARKET_B).await.is_none());

    let stats = running.finish().await;
    assert_eq!(stats.markets_dropped, 1);
    assert!(
        stats.frames_unrouted + stats.fenced_events >= 1,
        "the fenced generation's frames were discarded, never applied"
    );
}

/// The venue's answer to a subscription is load-bearing nowhere.
///
/// This venue publishes no acknowledgment that can be correlated with the command that
/// provoked it (`docs/limitless.md`), so nothing here waits for one. A peer that never sends
/// a `system` answer at all still gets a shard that establishes, routes frames, and carries
/// a set change on a fresh connection.
#[tokio::test]
async fn a_set_change_is_reconciled_without_any_venue_answer() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_namespace().await;
    assert_eq!(
        connection
            .expect_unacknowledged_resubscription(STEP_TIMEOUT)
            .await
            .slugs,
        owned(&[MARKET_A])
    );
    send_book(&mut connection, MARKET_A, "10", 1).await;
    await_book(
        &mut a,
        "market A live with no venue acknowledgment",
        is_live,
    )
    .await;

    running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let mut replacement = peer.next_connection().await;
    replacement.complete_namespace().await;
    assert_eq!(
        replacement
            .expect_unacknowledged_resubscription(STEP_TIMEOUT)
            .await
            .slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the replacement is dialled on the shard's own decision, not on a venue answer"
    );
    send_book(&mut replacement, MARKET_A, "11", 2).await;
    send_book(&mut replacement, MARKET_B, "20", 3).await;
    await_book(&mut a, "market A live on the replacement", is_live).await;
    let mut b = running
        .observe(MARKET_B)
        .await
        .expect("market B has a book");
    await_book(&mut b, "market B live on the replacement", is_live).await;

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 2);
    assert_eq!(stats.set_replacements, 1);
}

/// Removing and re-adding a market across two set changes gives it a new writer each time,
/// and the incarnation the removal ended says so before it goes.
#[tokio::test]
async fn a_market_removed_and_added_again_comes_back_as_a_fresh_incarnation() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    send_book(&mut connection, MARKET_B, "21", 3).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B at its second book", |published| {
        published.revision() >= 2
    })
    .await;

    running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let ended = await_book(&mut b, "market B unsubscribed", |published| {
        matches!(published.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert!(
        ended.revision() > 2,
        "the incarnation the removal ended publishes its own last word"
    );

    let mut without = peer.next_connection().await;
    assert_eq!(without.complete_handshake().await.slugs, owned(&[MARKET_A]));
    running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let mut rejoined = peer.next_connection().await;
    assert_eq!(
        rejoined.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the net-unchanged flip still reconciles, because the set moved in between"
    );
    send_book(&mut rejoined, MARKET_B, "23", 4).await;
    let mut back = running
        .observe(MARKET_B)
        .await
        .expect("market B has a book again");
    let published = await_book(&mut back, "the re-added market's first base", is_live).await;
    assert_eq!(
        published.revision(),
        1,
        "a re-added market is a fresh incarnation, never the retained writer"
    );
    assert_eq!(published.continuity().epoch(), 0);

    let stats = running.finish().await;
    assert_eq!(stats.connection_attempts, 3);
    assert_eq!(stats.set_replacements, 2);
}

#[tokio::test]
async fn a_market_that_never_recovers_reaches_its_own_terminal_verdict() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        max_recovery_attempts: 2,
        ..test_config(peer.endpoint(), &[MARKET_A, MARKET_B])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    for round in 0..3u64 {
        let mut connection = peer.next_connection().await;
        connection.complete_handshake().await;
        send_book(&mut connection, MARKET_A, "10", round + 1).await;
        await_book(&mut a, "market A live on this connection", is_live).await;
        connection.drop_abruptly().await;
        await_book(&mut a, "market A stale on the lost rail", |published| {
            !is_live(published)
        })
        .await;
    }

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 9).await;
    let recovered = await_book(&mut a, "market A live again", is_live).await;
    assert!(recovered.continuity().epoch() >= 1);
    assert!(
        is_stale(&b.latest(), AuthorityReason::RecoveryBaseUnavailable),
        "a market the venue never served reaches its own terminal verdict, got {:?}",
        b.latest().authority()
    );

    let stats = running.finish().await;
    assert!(stats.recovery_base_unavailable >= 1);
}

#[tokio::test]
async fn add_and_remove_churn_on_a_shard_with_no_connection_retains_no_books() {
    let peer = ControlledPeer::start(patient_peer()).await;
    let running = Running::start(
        Shard::new(test_config(peer.endpoint(), &[])).expect("test shard configuration is valid"),
    );

    for index in 0..200u32 {
        let slug = format!("churn-market-{index:04}");
        running
            .handle
            .add(vec![slug.clone()])
            .await
            .expect("the shard is running");
        running
            .handle
            .remove(vec![slug])
            .await
            .expect("the shard is running");
    }
    let settled = running.status().await;
    assert_eq!(settled.desired, 0);
    assert!(
        settled.markets.is_empty(),
        "a market the venue never heard of is forgotten the instant nobody wants it, got {}",
        settled.markets.len()
    );

    let stats = running.finish().await;
    assert_eq!(stats.tombstones_evicted, 0);
}

#[tokio::test]
async fn a_storm_of_undecodable_frames_reports_one_rail_loss_per_generation() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;

    for version in 0..12u64 {
        connection
            .send_orderbook(MARKET_A, &[("2", "100")], &[("0.6", "200")], Some(version))
            .await;
    }
    await_book(&mut a, "market A Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;
    await_book(&mut b, "market B Stale(LocalLoss)", |published| {
        is_stale(published, AuthorityReason::LocalLoss)
    })
    .await;

    let stats = running.finish().await;
    assert_eq!(
        stats.continuity_losses, 2,
        "one loss per market for the whole storm, not one walk of the set per frame"
    );
    assert!(
        stats.decode_failures.values().sum::<u64>() >= 2,
        "the storm was many frames, and each undecodable one is counted"
    );
}

/// A removed market's observer reads the final state before the writer goes away, so a
/// consumer holding only latest state is never left with a book that still claims to be live.
#[tokio::test]
async fn a_removed_market_publishes_unsubscribed_before_its_writer_goes_away() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;
    let held = b.latest().revision();

    running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    let ended = await_book(&mut b, "market B unsubscribed", |published| {
        matches!(published.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert!(
        ended.revision() > held,
        "the final revision is published, so a reader is woken by it"
    );
    assert!(
        b.try_recv().is_err(),
        "the mutation stream closes with the writer, after the final state was published"
    );

    let stats = running.finish().await;
    assert_eq!(stats.markets_dropped, 1);
}

#[tokio::test]
async fn a_shard_refuses_a_set_larger_than_its_declared_capacity() {
    let shard = Shard::new(ShardConfig {
        markets: owned(&[MARKET_A, MARKET_B, MARKET_C]),
        max_markets: 2,
        ..ShardConfig::default()
    });
    assert!(shard.is_err());

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let running = Running::start(
        Shard::new(ShardConfig {
            max_markets: 1,
            ..test_config(peer.endpoint(), &[MARKET_A])
        })
        .expect("test shard configuration is valid"),
    );
    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;

    let refused = running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(
        refused[0].status,
        MarketStatus::Rejected(MarketRejection::CapacityExceeded)
    );
    connection.expect_no_subscription(NO_COMMAND_WINDOW).await;

    let stats = running.finish().await;
    assert_eq!(stats.subscriptions_emitted, 1);
}

/// A base the venue served and then took away is a failed recovery cycle, not a successful
/// one — here, lost to a rail failure that named no market.
///
/// Counting the generation that served the base as a success would let a market that keeps
/// losing authority reconnect for ever without reaching a verdict, because every cycle would
/// clear the attempts the last one spent.
#[tokio::test]
async fn a_base_lost_to_a_rail_failure_spends_a_recovery_attempt() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        max_recovery_attempts: 2,
        ..test_config(peer.endpoint(), &[MARKET_A])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    send_book(&mut first, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;
    first
        .send_orderbook(MARKET_A, &[("2", "100")], &[("0.6", "200")], Some(2))
        .await;
    await_book(
        &mut a,
        "market A stale on the frame it could not apply",
        |published| is_stale(published, AuthorityReason::LocalLoss),
    )
    .await;
    first.drop_abruptly().await;

    let mut second = peer.next_connection().await;
    second.complete_handshake().await;
    await_book(
        &mut a,
        "market A stale on a rail that serves it nothing",
        |published| !is_live(published),
    )
    .await;
    second.drop_abruptly().await;

    let verdict = await_book(&mut a, "market A's own terminal verdict", |published| {
        is_stale(published, AuthorityReason::RecoveryBaseUnavailable)
    })
    .await;
    assert!(
        matches!(verdict.continuity(), MutationContinuity::Lost { .. }),
        "the verdict is reported on a book whose stream is broken, not on a healthy one"
    );

    let stats = running.finish().await;
    assert!(stats.recovery_base_unavailable >= 1);
}

/// A market holding its terminal recovery verdict keeps it through a later rail loss.
///
/// An undecodable frame afterwards is a fact about the rail, and says nothing new about a
/// market the venue has stopped serving. Letting it overwrite the reason would tell an
/// operator that an unrecoverable market is merely disconnected.
#[tokio::test]
async fn a_terminal_market_keeps_its_verdict_through_a_later_rail_loss() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        max_recovery_attempts: 2,
        ..test_config(peer.endpoint(), &[MARKET_A, MARKET_B])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let mut b = shard.observe(MARKET_B).expect("market B is in the set");
    let running = Running::start(shard);

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    send_book(&mut first, MARKET_A, "10", 1).await;
    send_book(&mut first, MARKET_B, "20", 2).await;
    await_book(&mut a, "market A live", is_live).await;
    await_book(&mut b, "market B live", is_live).await;
    first.drop_abruptly().await;

    for round in 0..2u64 {
        let mut connection = peer.next_connection().await;
        connection.complete_handshake().await;
        send_book(&mut connection, MARKET_A, "10", round + 3).await;
        await_book(&mut a, "market A live again", is_live).await;
        connection.drop_abruptly().await;
        await_book(&mut a, "market A stale on the lost rail", |published| {
            !is_live(published)
        })
        .await;
    }
    await_book(&mut b, "market B's own terminal verdict", |published| {
        is_stale(published, AuthorityReason::RecoveryBaseUnavailable)
    })
    .await;

    let mut connection = peer.next_connection().await;
    connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "11", 9).await;
    await_book(&mut a, "market A live on the last rail", is_live).await;
    for version in 20..26u64 {
        connection
            .send_orderbook(MARKET_A, &[("2", "100")], &[("0.6", "200")], Some(version))
            .await;
    }
    await_book(
        &mut a,
        "market A stale on the undecodable storm",
        |published| is_stale(published, AuthorityReason::LocalLoss),
    )
    .await;
    assert!(
        is_stale(&b.latest(), AuthorityReason::RecoveryBaseUnavailable),
        "the terminal verdict survives a rail loss that says nothing new, got {:?}",
        b.latest().authority()
    );

    let stats = running.finish().await;
    assert!(stats.recovery_base_unavailable >= 1);
}

/// The same accounting when the loss was this one market's own: a snapshot the book refused.
///
/// The rail was healthy throughout and no other market lost anything, so nothing but this
/// market's own evidence says the cycle failed. It still spends an attempt.
#[tokio::test]
async fn a_base_lost_to_a_refused_snapshot_spends_a_recovery_attempt() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        max_recovery_attempts: 2,
        level_capacity: 2,
        ..test_config(peer.endpoint(), &[MARKET_A])
    })
    .expect("test shard configuration is valid");
    let mut a = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut first = peer.next_connection().await;
    first.complete_handshake().await;
    send_book(&mut first, MARKET_A, "10", 1).await;
    await_book(&mut a, "market A live", is_live).await;
    first
        .send_orderbook(
            MARKET_A,
            &[("0.52", "10"), ("0.51", "10")],
            &[("0.53", "10")],
            Some(2),
        )
        .await;
    await_book(
        &mut a,
        "market A stale on the snapshot it refused",
        |published| is_stale(published, AuthorityReason::LocalLoss),
    )
    .await;
    first.drop_abruptly().await;

    let mut second = peer.next_connection().await;
    second.complete_handshake().await;
    await_book(
        &mut a,
        "market A stale on a rail that serves it nothing",
        |published| !is_live(published),
    )
    .await;
    second.drop_abruptly().await;

    let verdict = await_book(&mut a, "market A's own terminal verdict", |published| {
        is_stale(published, AuthorityReason::RecoveryBaseUnavailable)
    })
    .await;
    assert!(matches!(
        verdict.continuity(),
        MutationContinuity::Lost { .. }
    ));

    let stats = running.finish().await;
    assert!(stats.recovery_base_unavailable >= 1);
}

/// A freshly formatted multi-market segment: the shard's half, and the region this test
/// attaches to as an ordinary consumer would.
struct Segment {
    region: Arc<SegmentRegion>,
    shard: Option<ShardSegment>,
    path: PathBuf,
}

impl Segment {
    /// Hands the writer half to the shard, leaving this test holding the region and the
    /// cleanup.
    fn take(&mut self) -> ShardSegment {
        self.shard.take().expect("the segment is handed over once")
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let _removed = std::fs::remove_file(self.path.as_path());
        let mut page = self.path.as_os_str().to_owned();
        page.push(".doorbell");
        let _removed = std::fs::remove_file(PathBuf::from(page));
    }
}

fn segment_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-shard-{name}-{}-{}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos()
    ));
    path
}

fn open_segment(name: &str, slots: u32, levels: u32) -> Segment {
    let path = segment_path(name);
    let layout = SegmentLayout::new(slots, slots, levels, SEGMENT_EVENTS, 16)
        .expect("a valid segment geometry");
    let region = Arc::new(
        SegmentRegion::create_file(path.as_path(), layout.region_size())
            .expect("create the segment file"),
    );
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, SEGMENT_INSTANCE, 1),
    )
    .expect("format the segment");
    Segment {
        region,
        shard: Some(ShardSegment::new(name.to_owned(), writer)),
        path,
    }
}

fn market(slug: &str) -> MarketRef {
    MarketRef::new(
        Venue::new(VENUE).expect("the venue name is valid"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), slug).expect("the slug is valid"),
    )
}

/// Waits until the segment's published state for `handle` satisfies `predicate`.
///
/// Read faults are this reader's own bound expiring against a live writer, so they are
/// retried rather than reported.
async fn await_slot(
    reader: &SegmentReader,
    handle: MarketHandle,
    what: &str,
    predicate: impl Fn(&BookSnapshot) -> bool,
) -> BookSnapshot {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut last = None;
    loop {
        if let Ok(snapshot) = reader.read(handle) {
            if predicate(&snapshot) {
                return snapshot;
            }
            last = Some(snapshot);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; last read {last:?}"
        );
        tokio::time::sleep(SEGMENT_POLL).await;
    }
}

/// Reads the next delivery out of `stream`, or panics naming what it was waiting for.
async fn next_event(stream: &mut EventStream, what: &str) -> RetainedEvent {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        match stream.poll() {
            Ok(EventPoll::Delivered(event)) => return event,
            Ok(EventPoll::Idle) => {}
            Err(StreamFault::ContinuityLost { reason }) => {
                panic!("the consumer lost continuity waiting for {what}: {reason:?}")
            }
            Err(StreamFault::Read(_)) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what} at {:?}",
            stream.cursor()
        );
        tokio::time::sleep(SEGMENT_POLL).await;
    }
}

/// Every book a shard holds is published into that shard's own segment: latest state and
/// level mutations, each carrying the arrival stamp of the venue frame that produced it.
///
/// The frames are served to two markets on one connection, so what is proven is the routing
/// as well as the publication: each market's state lands in its own slot and each market's
/// mutations in its own ring.
#[tokio::test]
async fn a_shard_publishes_every_market_it_holds_into_its_own_segment() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut segment = open_segment("round-trip", 8, 16);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let mut shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("test shard configuration is valid");
    shard
        .publish_into(segment.take())
        .expect("a segment sized for this shard takes every book it holds");
    let running = Running::start(shard);

    let a = reader
        .resolve(&market(MARKET_A))
        .expect("market A is installed");
    let b = reader
        .resolve(&market(MARKET_B))
        .expect("market B is installed");
    assert_ne!(a.state_slot_index(), b.state_slot_index(), "one slot each");
    let (attached, mut stream) = reader.attach_stream(a).expect("attach a stream");
    assert_eq!(
        attached.revision(),
        0,
        "the segment carries each book's initial revision before any frame"
    );

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    send_book(&mut connection, MARKET_A, "10", 1).await;
    send_book(&mut connection, MARKET_B, "20", 2).await;

    let based = await_slot(&reader, a, "market A's base in the segment", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    assert_eq!(based.market(), &market(MARKET_A));
    assert!(
        based.arrival_time_nanos().is_some_and(|stamp| stamp > 0),
        "the state slot carries the real arrival stamp of the frame that drove it"
    );
    assert!(based.commit_time_nanos().is_some_and(|stamp| stamp > 0));
    let other = await_slot(&reader, b, "market B's base in the segment", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    assert_eq!(other.market(), &market(MARKET_B));

    send_book(&mut connection, MARKET_A, "11", 3).await;
    let event = next_event(&mut stream, "market A's first mutation").await;
    assert_eq!(event.market(), &market(MARKET_A));
    assert_eq!(event.cursor(), &MutationCursor::new(0, 0));
    assert!(
        event.arrival_time_nanos().is_some_and(|stamp| stamp > 0),
        "a mutation carries the arrival stamp of the frame it was derived from"
    );

    let moved = await_slot(&reader, a, "market A's second revision", |snapshot| {
        snapshot.revision() >= 2
    })
    .await;
    assert!(matches!(moved.authority(), AuthorityState::Live));

    let stats = running.finish().await;
    assert_eq!(stats.snapshots_applied, 3);
    assert_eq!(stats.segment_failure, None);
}

/// A venue resolution reaches the segment's ring as its own delivery, and the state that
/// follows it names the position past it without moving the book.
///
/// The republished state is what a late attacher starts from, so it must be strictly past
/// the delivery already made; the revision must not move, because a resolution commits none.
#[tokio::test]
async fn a_resolution_reaches_the_segment_ring_and_the_state_past_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut segment = open_segment("resolution", 4, 16);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let mut shard =
        Shard::new(test_config(peer.endpoint(), &[MARKET_A])).expect("a valid configuration");
    shard.publish_into(segment.take()).expect("install");
    let running = Running::start(shard);

    let handle = reader.resolve(&market(MARKET_A)).expect("installed");
    let (_attached, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _request = connection.complete_handshake().await;
    send_book(&mut connection, MARKET_A, "10", 1).await;
    let based = await_slot(&reader, handle, "the base", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    let committed = based.commit_time_nanos();

    connection
        .send_market_resolved(MARKET_A, "clob", "No", 1, RESOLUTION_DATE)
        .await;
    let event = next_event(&mut stream, "the resolution in the ring").await;
    let RetainedEvent::Resolution(resolution) = event else {
        panic!("the ring carried {event:?} where a resolution was published");
    };
    assert_eq!(resolution.winning_outcome(), "No");
    assert_eq!(resolution.winning_index(), 1);
    assert_eq!(resolution.market_type(), "clob");
    assert_eq!(resolution.resolution_date(), RESOLUTION_DATE);
    assert_eq!(
        resolution.revision(),
        based.revision(),
        "a resolution names the revision it was ordered after and advances none"
    );
    assert!(
        resolution.arrival_time_nanos().is_some_and(|at| at > 0),
        "the ring entry carries the resolution frame's own arrival stamp"
    );

    let after = await_slot(
        &reader,
        handle,
        "the state past the resolution",
        |snapshot| {
            snapshot.continuity()
                == &MutationContinuity::Intact {
                    epoch: 0,
                    next_position: 1,
                }
        },
    )
    .await;
    assert_eq!(
        after.revision(),
        based.revision(),
        "the republished state is the same book revision"
    );
    assert_eq!(
        after.commit_time_nanos(),
        committed,
        "the republish carries the commit stamp forward rather than restamping a book that \
         has not moved"
    );

    send_book(&mut connection, MARKET_A, "11", 2).await;
    let next = next_event(&mut stream, "the mutation after the resolution").await;
    assert_eq!(
        next.cursor(),
        &MutationCursor::new(0, 1),
        "book flow resumes at the position after the resolution's"
    );

    let stats = running.finish().await;
    assert_eq!(stats.resolutions_forwarded, 1);
    assert_eq!(stats.events_resolved, 1);
    assert_eq!(stats.segment_failure, None);
}

/// A market taken out of the desired set publishes its final state — no longer subscribed —
/// into the segment before its book is dropped, so a consumer holding the slot reads the
/// book's own last word rather than a state that says `Live` forever.
#[tokio::test]
async fn a_removed_market_publishes_unsubscribed_into_the_segment_before_its_book_goes_away() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut segment = open_segment("removal", 4, 16);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let mut shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("a valid configuration");
    shard.publish_into(segment.take()).expect("install");
    let running = Running::start(shard);

    let handle = reader.resolve(&market(MARKET_B)).expect("installed");
    let mut connection = peer.next_connection().await;
    let _request = connection.complete_handshake().await;
    send_book(&mut connection, MARKET_B, "20", 1).await;
    let live = await_slot(
        &reader,
        handle,
        "market B live in the segment",
        |snapshot| matches!(snapshot.authority(), AuthorityState::Live),
    )
    .await;

    let removed = running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(removed[0].status, MarketStatus::Removed);

    let final_state = await_slot(&reader, handle, "market B unsubscribed", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert!(
        final_state.revision() > live.revision(),
        "the final state is a revision of its own, not the live one relabelled"
    );

    let status = running.status().await;
    assert_eq!(
        status.segment_markets, 1,
        "the removed market's entry is retired and never reused"
    );
    assert_eq!(status.segment.as_deref(), Some("removal"));

    let stats = running.finish().await;
    assert_eq!(stats.segment_failure, None);
}

/// A publication the segment refuses ends the run loudly rather than leaving consumers
/// reading a segment whose writer silently stopped.
///
/// The refusal is forced with a segment shallower than the books the shard accepts, which no
/// configuration this daemon validates can produce — `daemon` sizes the slots from the same
/// depth the books take — so what is proven is the fail-closed behavior, not a reachable
/// misconfiguration.
#[tokio::test]
async fn a_refused_segment_publication_ends_the_shard_loudly() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut segment = open_segment("refusal", 4, 1);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let mut shard =
        Shard::new(test_config(peer.endpoint(), &[MARKET_A])).expect("a valid configuration");
    shard
        .publish_into(segment.take())
        .expect("an empty book fits a one-level slot");
    let handle = reader.resolve(&market(MARKET_A)).expect("installed");
    let stop = shard.stopper();
    let task = tokio::spawn(async move {
        let mut shard = shard;
        let stats = shard.run_until(Some(Instant::now() + RUN_CAP)).await;
        (stats, shard.segment_failure().cloned())
    });

    let mut connection = peer.next_connection().await;
    let _request = connection.complete_handshake().await;
    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;

    let (stats, failure) = tokio::time::timeout(STEP_TIMEOUT, task)
        .await
        .expect("a refused publication ends the run without waiting for the deadline")
        .expect("the shard task completes");
    stop.stop();
    assert!(
        matches!(failure, Some(WriterError::LevelCapacityExceeded { .. })),
        "the run must end on the refusal that caused it, not on its own clock: {failure:?}"
    );
    assert!(
        stats.segment_failure.is_some(),
        "the refusal is reported in what the run returns"
    );
    let snapshot = reader.read(handle).expect("the slot is still readable");
    assert_eq!(
        snapshot.revision(),
        0,
        "a refused publication leaves the slot at the revision it last published"
    );
}

/// A market removed and added again resumes through its own delivery entry, rebased.
///
/// This is what makes demand that comes and goes — a lease taken, released, and taken again —
/// serviceable at all. The entry is never given to a *different* market, which is the ABI's
/// identity rule and is untouched; the same market coming back to its own entry is not
/// identity reuse, and the ABI already carries the cell that keeps it honest. So what is
/// pinned here is the whole observable sequence a consumer sees on one directory index: live,
/// then an unsubscribed interval nothing can mistake for a book, then live again under a
/// continuity epoch strictly past the retired one — and, for a consumer that was reading the
/// ring across all of it, an explicit continuity loss rather than one spliced event.
#[tokio::test]
async fn a_re_added_market_resumes_on_its_own_entry_under_a_new_epoch() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut segment = open_segment("re-add", 8, 16);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let mut shard = Shard::new(test_config(peer.endpoint(), &[MARKET_A, MARKET_B]))
        .expect("a valid configuration");
    shard.publish_into(segment.take()).expect("install");
    let running = Running::start(shard);

    let b = reader.resolve(&market(MARKET_B)).expect("installed");
    let mut connection = peer.next_connection().await;
    let _request = connection.complete_handshake().await;
    send_book(&mut connection, MARKET_B, "20", 1).await;
    let live = await_slot(&reader, b, "market B live", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Live)
    })
    .await;
    let retired_epoch = live.continuity().epoch();
    let (_attached, mut stream) = reader.attach_stream(b).expect("a stream over market B");
    send_book(&mut connection, MARKET_B, "21", 2).await;
    let _mutation = next_event(&mut stream, "a mutation on the first incarnation").await;

    let removed = running
        .handle
        .remove(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(removed[0].status, MarketStatus::Removed);
    let unsubscribed = await_slot(&reader, b, "market B unsubscribed", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Unsubscribed)
    })
    .await;
    assert_eq!(
        unsubscribed.continuity().epoch(),
        retired_epoch,
        "the retired entry is left standing at the epoch its incarnation published from"
    );
    assert_eq!(
        running.status().await.segment_markets,
        1,
        "a retired entry holds no live market"
    );

    let mut shrunk = peer.next_connection().await;
    assert_eq!(
        shrunk.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "the removal reaches the venue as the set that is left"
    );

    let resumed = running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(
        resumed[0].status,
        MarketStatus::Accepted,
        "a market that has come and gone is taken back"
    );
    assert_eq!(
        reader.resolve(&market(MARKET_B)).expect("installed"),
        b,
        "it comes back through its own entry, not a second one"
    );
    assert_eq!(
        running.status().await.segment_markets,
        2,
        "the resumed entry is live again"
    );

    let mut regrown = peer.next_connection().await;
    assert_eq!(
        regrown.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the resumed market is subscribed again"
    );
    send_book(&mut regrown, MARKET_B, "22", 3).await;
    let relived = await_slot(&reader, b, "market B live again", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Live)
    })
    .await;
    assert!(
        relived.continuity().epoch() > retired_epoch,
        "the resumed incarnation publishes from an epoch past the retired one: {} then {}",
        retired_epoch,
        relived.continuity().epoch()
    );
    assert!(
        relived.revision() > unsubscribed.revision(),
        "the slot's book revision never goes backwards across a resume"
    );

    match stream.poll() {
        Err(StreamFault::ContinuityLost { reason }) => assert!(
            matches!(
                reason,
                ContinuityReason::Reconnect | ContinuityReason::RecoveryBase
            ),
            "a stream across a resume is told it rebased, not why some other break happened: \
             {reason:?}"
        ),
        other => panic!(
            "a stream held across a resume must never deliver across it, and never idle \
             forever: {other:?}"
        ),
    }
    let rebased = stream.reattach().expect("the stream reattaches");
    assert_eq!(
        rebased.continuity().epoch(),
        relived.continuity().epoch(),
        "reattaching lands on the resumed incarnation's own stream"
    );

    send_book(&mut regrown, MARKET_A, "10", 4).await;
    let a = reader.resolve(&market(MARKET_A)).expect("installed");
    let _still_serving = await_slot(&reader, a, "market A still served", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Live)
    })
    .await;

    let stats = running.finish().await;
    assert_eq!(stats.segment_failure, None);
}
