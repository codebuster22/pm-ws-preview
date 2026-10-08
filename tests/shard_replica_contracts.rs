#![forbid(unsafe_code)]

//! Hot-standby redundancy contracts for the real `limitless::shard`, driven against the
//! scripted controlled peer.
//!
//! `replicas > 1` is active-active across the venue-key gate wherever the configured
//! declaration licenses it, so this file configures a declaration that licenses nothing —
//! `dedup-only`, the tier `docs/design.md` gives a venue whose key is evidence of frame
//! identity and of no ordering at all. What that produces is exactly the topology every
//! contract here is about: one publishing primary, hot standbys shadowing it, and the
//! promotion question asked once per market when the publishing connection is lost. It is
//! also what an armed pool hands back to when its live tripwire fires, so these are the
//! contracts of the degraded state as much as of the unlicensed one; the transition into it,
//! and one whole promotion cycle on the far side, are in `shard_pool_contracts.rs`.
//!
//! One peer serves both of a shard's connections; each accepted connection is scripted
//! separately, so a test decides exactly what the primary and the standby each see. Which
//! connection the shard assigned to which role is read from [`ShardStatus::connections`] by
//! Engine.IO session id rather than assumed from accept ordering, so nothing here depends on
//! a race. Every book assertion reads through a [`BookObserver`] attached to that market's
//! own writer, which is the surface a consumer reads.
//!
//! The shard rail's promotion question is per market, which is what separates these
//! contracts from `replica_contracts.rs`: one socket carries a whole set, so losing it asks
//! the promotion question once per market in that set, and a set can answer it both ways at
//! once.

mod support;

use pm_ws::limitless::shard::{
    ConnectionReport, MarketStatus, Shard, ShardConfig, ShardError, ShardHandle, ShardMetrics,
    ShardStats, ShardStatus, ShardStopper, SubscriptionState,
};
use pm_ws::{
    AuthorityReason, AuthorityState, BookObserver, DedupKeyDeclaration, DedupKeySemantics,
    MutationContinuity, PublishedBook, ReplicaRole, Side,
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
/// Longer than the daemon's default 500 ms command floor, so a connection that was going to
/// be dialled has certainly been dialled by the time a negative assertion gives up.
const NO_CONNECTION_WINDOW: Duration = Duration::from_millis(900);
/// The configured command floor, less the scheduling slack between the instant a command is
/// authorized and the instant its bytes reach the peer.
const PACING_FLOOR: Duration = Duration::from_millis(450);

/// A peer whose announced heartbeat cadence is far longer than any test step, so a test
/// about redundancy cannot be disturbed by a heartbeat deadline.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// A venue key declaration that licenses no pooled publishing, so two connections are one
/// publishing primary and one hot standby.
const DEDUP_ONLY: DedupKeyDeclaration = DedupKeyDeclaration::new(
    "limitless",
    "orderbookUpdate",
    "version",
    DedupKeySemantics::DedupOnly,
);

/// Production policy with the reconnect clock compressed and one hot standby requested.
fn replica_config(endpoint: String, markets: &[&str]) -> ShardConfig {
    ShardConfig {
        replicas: 2,
        dedup_key: DEDUP_ONLY,
        ..single_config(endpoint, markets)
    }
}

/// The same policy with the shipped default replica count, which is one connection.
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

/// Waits until the shard's status satisfies `predicate`, or panics naming what it was
/// waiting for and the connection roles it last saw.
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
                "timed out waiting for {what}; last saw {:?}",
                status.connections
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits until the shard's own counters satisfy `predicate`, or panics naming what it was
/// waiting for. A shard drains its ingest queue on its own schedule, so a count that a
/// finished run would report is waited for rather than read after the run has stopped.
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

fn subscription_of(status: &ShardStatus, slug: &str) -> Option<SubscriptionState> {
    status
        .markets
        .iter()
        .find(|report| report.slug == slug)
        .map(|report| report.subscription)
}

/// The connection row holding the publishing role, if one does.
fn primary_row(status: &ShardStatus) -> Option<&ConnectionReport> {
    status
        .connections
        .iter()
        .find(|row| row.role == ReplicaRole::PublishingPrimary)
}

/// The connection rows shadowing the publishing one.
fn standby_rows(status: &ShardStatus) -> Vec<&ConnectionReport> {
    status
        .connections
        .iter()
        .filter(|row| row.role == ReplicaRole::HotStandby)
        .collect()
}

/// The generation currently publishing, when an established connection holds that role.
fn publishing_generation(status: &ShardStatus) -> Option<u64> {
    primary_row(status)
        .filter(|row| row.established)
        .map(|row| row.generation)
}

/// The role the shard reports for the connection holding `session`, once it has one.
fn role_of(status: &ShardStatus, session: &str) -> Option<ReplicaRole> {
    status
        .connections
        .iter()
        .find(|row| row.session.as_deref() == Some(session))
        .map(|row| row.role.clone())
}

/// Sends one distinguishable book for `slug`, sized so each market's levels are unique.
async fn send_book(connection: &mut PeerConnection, slug: &str, size: &str, version: u64) {
    connection
        .send_orderbook(slug, &[("0.51", size)], &[("0.52", size)], Some(version))
        .await;
}

/// Names two accepted connections (primary, standby) by the role the shard reports for each
/// Engine.IO session id, rather than by the order the listener accepted them.
///
/// Every role dials on its own schedule and nothing orders one role's dial ahead of
/// another's, so which of two sockets the listener accepts first is the runtime's business
/// and not a property of the shard. A test naming them by accept order reads one role's
/// frames as the other's wherever the two dials are scheduled the other way round.
async fn named_by_role(
    running: &Running,
    first: PeerConnection,
    second: PeerConnection,
) -> (PeerConnection, PeerConnection) {
    let first_sid = first.engine_sid();
    let second_sid = second.engine_sid();
    let status = await_status(running, "both connections to announce a role", |status| {
        role_of(status, &first_sid).is_some() && role_of(status, &second_sid).is_some()
    })
    .await;
    let first_role = role_of(&status, &first_sid).expect("the shard named the first role");
    let second_role = role_of(&status, &second_sid).expect("the shard named the second role");
    match (first_role, second_role) {
        (ReplicaRole::PublishingPrimary, ReplicaRole::HotStandby) => (first, second),
        (ReplicaRole::HotStandby, ReplicaRole::PublishingPrimary) => (second, first),
        (left, right) => panic!("expected one primary and one standby, got {left:?} and {right:?}"),
    }
}

/// Accepts and completes both of the shard's connections, then returns them ordered
/// (primary, standby) by the role the shard reported for each.
async fn connect_pair(
    peer: &mut ControlledPeer,
    running: &Running,
    markets: &[&str],
) -> (PeerConnection, PeerConnection) {
    let mut first = peer.next_connection().await;
    assert_eq!(
        first.complete_handshake().await.slugs,
        owned(markets),
        "the first connection subscribes the whole desired set"
    );
    let first_subscribed = Instant::now();
    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        owned(markets),
        "the second connection subscribes the same replace-set as the first"
    );
    let gap = Instant::now().saturating_duration_since(first_subscribed);
    assert!(
        gap >= PACING_FLOOR,
        "one endpoint's command floor paces both roles' subscriptions at the wire; \
         the second landed {gap:?} after the first"
    );
    named_by_role(running, first, second).await
}

/// Accepts and completes the pair of connections a desired-set change dials, ordered
/// (primary, standby) by the role the shard reported for each.
async fn redialled_pair(
    peer: &mut ControlledPeer,
    running: &Running,
    markets: &[&str],
) -> (PeerConnection, PeerConnection) {
    let mut first = peer.next_connection().await;
    assert_eq!(
        first.complete_handshake().await.slugs,
        owned(markets),
        "every role redials the whole new set"
    );
    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        owned(markets),
        "every role redials the whole new set"
    );
    named_by_role(running, first, second).await
}

#[tokio::test]
async fn an_agreeing_standby_takes_over_a_multi_market_shard_without_a_book_leaving_live() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B, MARKET_C];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut books = [
        shard.observe(MARKET_A).expect("market A is in the set"),
        shard.observe(MARKET_B).expect("market B is in the set"),
        shard.observe(MARKET_C).expect("market C is in the set"),
    ];
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    for (index, slug) in markets.iter().enumerate() {
        let version = index as u64 + 1;
        send_book(&mut primary, slug, "10", version).await;
        send_book(&mut standby, slug, "10", version).await;
    }
    let mut bases = Vec::new();
    for (index, slug) in markets.iter().enumerate() {
        bases.push(await_book(&mut books[index], &format!("{slug} live"), is_live).await);
    }
    await_status(&running, "every shadow agreeing with its book", |status| {
        standby_rows(status)
            .first()
            .is_some_and(|row| row.agreeing_markets == Some(markets.len()))
    })
    .await;
    let standby_row_generation = standby_rows(&running.status().await)
        .first()
        .expect("a standby row is reported")
        .generation;

    primary.drop_abruptly().await;

    let promoted = await_status(
        &running,
        "the standby taking over the publishing role",
        |status| publishing_generation(status) == Some(standby_row_generation),
    )
    .await;
    assert!(
        standby_rows(&promoted)
            .iter()
            .all(|row| row.generation != standby_row_generation),
        "the promoted connection left the standby role rather than holding both: {:?}",
        promoted.connections
    );

    for (index, slug) in markets.iter().enumerate() {
        send_book(&mut standby, slug, "20", index as u64 + 10).await;
    }
    for (index, slug) in markets.iter().enumerate() {
        let after = await_book(
            &mut books[index],
            &format!("a snapshot {slug} committed by the promoted source"),
            |published| published.revision() > bases[index].revision(),
        )
        .await;
        assert!(
            is_live(&after),
            "{slug} was never stale across the switch: {:?}",
            after.authority()
        );
        assert_eq!(
            after.revision(),
            bases[index].revision() + 1,
            "{slug}: a staleness report would have spent a revision between the base and this \
             snapshot"
        );
        assert!(
            matches!(
                after.continuity(),
                MutationContinuity::Intact { epoch: 0, .. }
            ),
            "{slug}: a seamless source switch opens no new epoch: {:?}",
            after.continuity()
        );
        assert_eq!(
            levels(&after, Side::Bid),
            expect_levels(&[("0.51", "20")]),
            "{slug} carries what the promoted source published"
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
        "one source switch was counted for the shard"
    );
    assert_eq!(
        stats.markets_promoted,
        markets.len() as u64,
        "every market in the set carried its own promotion evidence"
    );
    assert_eq!(
        stats.markets_promotion_refused, 0,
        "no market was refused when every shadow agreed"
    );
}

#[tokio::test]
async fn a_diverged_market_is_refused_while_its_agreeing_siblings_promote() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B, MARKET_C];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut books = [
        shard.observe(MARKET_A).expect("market A is in the set"),
        shard.observe(MARKET_B).expect("market B is in the set"),
        shard.observe(MARKET_C).expect("market C is in the set"),
    ];
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    for (index, slug) in markets.iter().enumerate() {
        send_book(&mut primary, slug, "10", index as u64 + 1).await;
    }
    send_book(&mut standby, MARKET_A, "10", 1).await;
    standby
        .send_orderbook(MARKET_B, &[("0.51", "77")], &[("0.52", "77")], Some(2))
        .await;
    send_book(&mut standby, MARKET_C, "10", 3).await;

    let mut bases = Vec::new();
    for (index, slug) in markets.iter().enumerate() {
        bases.push(await_book(&mut books[index], &format!("{slug} live"), is_live).await);
    }
    await_status(
        &running,
        "the standby agreeing on exactly the two undisturbed markets",
        |status| {
            standby_rows(status)
                .first()
                .is_some_and(|row| row.agreeing_markets == Some(2))
        },
    )
    .await;
    let standby_generation = standby_rows(&running.status().await)
        .first()
        .expect("a standby row is reported")
        .generation;

    primary.drop_abruptly().await;

    await_status(
        &running,
        "the standby taking over the publishing role",
        |status| publishing_generation(status) == Some(standby_generation),
    )
    .await;

    let a = await_book(&mut books[0], "market A staying live", is_live).await;
    assert_eq!(
        a.revision(),
        bases[0].revision(),
        "market A's agreeing shadow took over without spending a revision"
    );
    let c = await_book(&mut books[2], "market C staying live", is_live).await;
    assert_eq!(
        c.revision(),
        bases[2].revision(),
        "market C's agreeing shadow took over without spending a revision"
    );
    let b = await_book(
        &mut books[1],
        "market B refusing promotion on a divergent shadow",
        |published| is_stale(published, AuthorityReason::ReplicaDivergence),
    )
    .await;
    assert!(
        b.revision() > bases[1].revision(),
        "the refusal is a published revision of its own"
    );
    assert_eq!(
        levels(&b, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "a refused market keeps the last state its own primary published, never the \
         standby's unproven one"
    );

    let reissue = standby.expect_resubscription(STEP_TIMEOUT).await;
    assert_eq!(
        reissue.slugs,
        owned(&markets),
        "the refused market recovers through the shard's existing whole-set reissue rail"
    );
    standby
        .send_orderbook(MARKET_B, &[("0.51", "31")], &[("0.52", "31")], Some(9))
        .await;
    let rebased = await_book(
        &mut books[1],
        "market B rebased on a fresh venue base",
        is_live,
    )
    .await;
    assert!(
        matches!(
            rebased.continuity(),
            MutationContinuity::Intact { epoch: 1, .. }
        ),
        "the refused market's recovery base opens a new continuity epoch: {:?}",
        rebased.continuity()
    );
    assert_eq!(
        levels(&rebased, Side::Bid),
        expect_levels(&[("0.51", "31")]),
        "market B carries the fresh base the promoted connection served"
    );

    let stats = running.finish().await;
    assert_eq!(stats.source_switches, 1, "one source switch was counted");
    assert_eq!(
        stats.markets_promoted, 2,
        "the two agreeing markets promoted"
    );
    assert_eq!(
        stats.markets_promotion_refused, 1,
        "the divergent market was refused promotion"
    );
}

#[tokio::test]
async fn a_standby_that_delivered_nothing_is_refused_without_naming_divergence() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut primary, _standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut primary, MARKET_A, "10", 1).await;
    let base = await_book(&mut book, "market A live", is_live).await;

    primary.drop_abruptly().await;

    let stale = await_book(&mut book, "market A losing authority", |published| {
        !is_live(published)
    })
    .await;
    assert_eq!(
        stale.authority(),
        &AuthorityState::Stale(AuthorityReason::Disconnect),
        "a shadow holding no comparable history reports the reason the connection ended \
         for, never replica divergence"
    );
    assert!(
        stale.revision() > base.revision(),
        "the refusal is a published revision of its own"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.source_switches, 1,
        "the viable standby connection still took over the publishing role: what it could \
         not do is carry any market's authority across the switch"
    );
    assert_eq!(
        stats.markets_promoted, 0,
        "a shadow holding no accepted base carries nothing across"
    );
    assert_eq!(
        stats.markets_promotion_refused, 1,
        "the market was refused promotion"
    );
}

#[tokio::test]
async fn a_replacement_connection_joins_as_standby_and_there_is_no_failback() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut primary, MARKET_A, "10", 1).await;
    send_book(&mut standby, MARKET_A, "10", 1).await;
    let base = await_book(&mut book, "market A live", is_live).await;
    await_status(&running, "the shadow agreeing", |status| {
        standby_rows(status)
            .first()
            .is_some_and(|row| row.agreeing_markets == Some(1))
    })
    .await;
    let promoted_generation = standby_rows(&running.status().await)
        .first()
        .expect("a standby row is reported")
        .generation;

    primary.drop_abruptly().await;
    await_status(&running, "the standby taking over", |status| {
        publishing_generation(status) == Some(promoted_generation)
    })
    .await;

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&markets),
        "standby coverage is rebuilt with the same desired market set"
    );
    let replacement_sid = replacement.engine_sid();
    let status = await_status(&running, "the replacement's announced role", |status| {
        role_of(status, &replacement_sid).is_some()
    })
    .await;
    assert_eq!(
        role_of(&status, &replacement_sid),
        Some(ReplicaRole::HotStandby),
        "a replacement connection joins as a standby, never as a second publisher"
    );
    assert_eq!(
        publishing_generation(&status),
        Some(promoted_generation),
        "the promoted connection keeps the publishing role: there is no failback"
    );

    send_book(&mut replacement, MARKET_A, "10", 1).await;
    await_status(&running, "the replacement shadow agreeing", |status| {
        standby_rows(status)
            .first()
            .is_some_and(|row| row.agreeing_markets == Some(1))
    })
    .await;
    let settled = running.status().await;
    assert_eq!(
        publishing_generation(&settled),
        Some(promoted_generation),
        "an agreeing replacement standby still does not displace the promoted primary"
    );

    send_book(&mut standby, MARKET_A, "40", 2).await;
    let after = await_book(
        &mut book,
        "the promoted source publishing again",
        |published| published.revision() > base.revision(),
    )
    .await;
    assert_eq!(
        levels(&after, Side::Bid),
        expect_levels(&[("0.51", "40")]),
        "the promoted connection is the one publishing"
    );
    let _ = running.finish().await;
}

#[tokio::test]
async fn a_standby_writes_no_published_book_and_a_fenced_generation_reaches_none() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A, MARKET_B];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let handle = shard.handle();
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut primary, MARKET_A, "10", 1).await;
    send_book(&mut primary, MARKET_B, "10", 2).await;
    let base = await_book(&mut book, "market A live", is_live).await;

    standby
        .send_orderbook(MARKET_A, &[("0.51", "99")], &[("0.52", "99")], Some(5))
        .await;
    send_book(&mut standby, MARKET_B, "10", 6).await;
    await_status(
        &running,
        "the shadow set agreeing on exactly the undisturbed market",
        |status| {
            standby_rows(status)
                .first()
                .is_some_and(|row| row.agreeing_markets == Some(1))
        },
    )
    .await;
    let held = book.latest();
    assert_eq!(
        held.revision(),
        base.revision(),
        "a standby arrival spent no revision on the published book"
    );
    assert_eq!(
        levels(&held, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the published book carries only what the publishing connection served"
    );

    let outcomes = handle
        .add(vec![MARKET_C.to_owned()])
        .await
        .expect("the shard takes the add");
    assert_eq!(outcomes.len(), 1, "one market was named");
    let mut fenced_primary = primary;
    let (mut replacement_primary, _replacement_standby) =
        redialled_pair(&mut peer, &running, &[MARKET_A, MARKET_B, MARKET_C]).await;

    fenced_primary
        .send_orderbook(MARKET_A, &[("0.51", "1234")], &[("0.52", "1234")], Some(77))
        .await;
    standby
        .send_orderbook(MARKET_A, &[("0.51", "4321")], &[("0.52", "4321")], Some(78))
        .await;
    send_book(&mut replacement_primary, MARKET_B, "55", 9).await;
    let mut market_b = handle
        .observe(MARKET_B)
        .await
        .expect("the shard is running")
        .expect("market B is in the set");
    await_book(
        &mut market_b,
        "the replacement generation publishing",
        |published| levels(published, Side::Bid) == expect_levels(&[("0.51", "55")]),
    )
    .await;

    let unmoved = book.latest();
    assert_ne!(
        levels(&unmoved, Side::Bid),
        expect_levels(&[("0.51", "1234")]),
        "a fenced generation's frame never reached the book"
    );
    assert_ne!(
        levels(&unmoved, Side::Bid),
        expect_levels(&[("0.51", "4321")]),
        "a fenced standby generation's frame never reached the book either"
    );

    await_metrics(
        &running,
        "both fenced generations' frames counted and discarded",
        |metrics| metrics.stats.fenced_events >= 2,
    )
    .await;
    let _ = running.finish().await;
}

#[tokio::test]
async fn the_default_configuration_runs_exactly_one_connection() {
    assert_eq!(
        ShardConfig::default().replicas,
        1,
        "the shipped default is today's single-connection behavior"
    );

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(single_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&markets),
        "the one connection carries the whole set"
    );
    peer.expect_no_connection(NO_CONNECTION_WINDOW).await;

    send_book(&mut connection, MARKET_A, "10", 1).await;
    let base = await_book(&mut book, "market A live", is_live).await;

    let status = await_status(&running, "the one connection's role", |status| {
        primary_row(status).is_some_and(|row| row.session.is_some())
    })
    .await;
    assert_eq!(
        status.connections.len(),
        1,
        "a default shard reports exactly one connection row"
    );
    assert_eq!(
        standby_rows(&status).len(),
        0,
        "a default shard runs no standby"
    );
    assert_eq!(
        primary_row(&status).and_then(|row| row.agreeing_markets),
        None,
        "the publishing connection maintains no shadow to agree with"
    );
    assert_eq!(
        subscription_of(&status, MARKET_A),
        Some(SubscriptionState::Established)
    );
    assert_eq!(
        status.replicas, 1,
        "a default shard names the single-connection topology it was configured for"
    );
    assert_eq!(
        status.standby_agreeing_markets, 0,
        "no standby is configured, so no market has promotion coverage"
    );
    let metrics = running.metrics().await;
    assert_eq!(metrics.replicas, 1);
    assert_eq!(
        metrics.standbys_established, 0,
        "a default shard holds no standby connection"
    );
    assert_eq!(metrics.standby_agreeing_markets, 0);
    assert!(
        metrics.stats.standby_ends.is_empty(),
        "a default shard has no standby ends to report"
    );

    connection.drop_abruptly().await;
    let stale = await_book(&mut book, "the single source's loss", |published| {
        !is_live(published)
    })
    .await;
    assert_eq!(
        stale.authority(),
        &AuthorityState::Stale(AuthorityReason::Disconnect),
        "losing the only connection reports the connection's own reason, exactly as before"
    );
    assert!(stale.revision() > base.revision());

    let mut recovered = peer.next_connection().await;
    assert_eq!(
        recovered.complete_handshake().await.slugs,
        owned(&markets),
        "the replacement carries the whole set"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.source_switches, 0,
        "a shard running no standby asks no promotion question"
    );
    assert_eq!(stats.markets_promoted, 0);
    assert_eq!(
        stats.markets_promotion_refused, 0,
        "a shard with no standby role refuses nothing: it has nothing to refuse"
    );
}

#[tokio::test]
async fn a_replica_count_outside_the_ceiling_is_refused_as_configuration() {
    let endpoint = "http://127.0.0.1:1".to_owned();
    assert_eq!(
        Shard::new(ShardConfig {
            replicas: 0,
            ..single_config(endpoint.clone(), &[MARKET_A])
        })
        .err(),
        Some(ShardError::ReplicasOutOfRange),
        "a shard publishing from no connection is a misconfiguration"
    );
    assert_eq!(
        Shard::new(ShardConfig {
            replicas: 5,
            ..single_config(endpoint.clone(), &[MARKET_A])
        })
        .err(),
        Some(ShardError::ReplicasOutOfRange),
        "the shard rail honors the same MAX_REPLICAS ceiling the single-market rail does"
    );
    assert!(
        Shard::new(ShardConfig {
            replicas: 4,
            ..single_config(endpoint, &[MARKET_A])
        })
        .is_ok(),
        "the ceiling itself is accepted"
    );
}

/// Waits for `predicate` on the book while keeping `connection`'s heartbeat evidence fresh.
///
/// A transition contract places exactly one deadline and reads what that one lapse
/// produced. Left unfed, the connection that took over expires on the same cadence a few
/// hundred milliseconds later and the book's next revision is that second lapse, not the
/// transition under test.
async fn await_book_while_feeding(
    connection: &mut PeerConnection,
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
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for {what}; last seen revision={} authority={:?}",
                latest.revision(),
                latest.authority()
            );
        }
        connection.send_ping().await;
        connection.expect_pong(STEP_TIMEOUT).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The heartbeat window the transition contracts below run in: long enough to set a scene
/// against, short enough that a run does not wait on a venue cadence. The harness never
/// sends a ping on a timer, so every heartbeat deadline here is placed by the test.
const TRANSITION_HEARTBEAT_MS: u64 = 1_500;

fn transition_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: TRANSITION_HEARTBEAT_MS,
        ping_timeout_ms: TRANSITION_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// A standby topology whose one drain cycle is withheld for `stall`, so a test can put a
/// frame in the ingest queue and know it is still there when a timer fires.
///
/// The stall arms on this shard's first accepted book state and fires once, so exactly one
/// market can hold a published base before the queue is held: these contracts are written
/// for one market for that reason.
fn stalled_replica_config(endpoint: String, markets: &[&str], stall: Duration) -> ShardConfig {
    ShardConfig {
        ingest_stall: Some(stall),
        ..replica_config(endpoint, markets)
    }
}

/// A standby frame already queued when the publishing connection's deadline lapses is
/// evidence the takeover is decided on, never work that replays over the book after it.
///
/// The hazard is an ordering one. The heartbeat deadline is polled ahead of the ingest
/// queue, so a standby snapshot can be sitting in that queue at the instant the timer fires.
/// Deciding promotion against the shadow as it stood *before* that snapshot promotes a
/// market the shard already holds a contradiction to, and then applies the contradiction to
/// the published book as an ordinary authoritative arrival: a book that moved across a
/// source switch with no continuity loss reported and no ordering proof for the move.
#[tokio::test]
async fn a_standby_frame_queued_at_the_deadline_is_evidence_before_the_takeover_not_after_it() {
    let mut peer = ControlledPeer::start(transition_peer()).await;
    let markets = [MARKET_A];
    let stall = Duration::from_millis(2_000);
    let shard = Shard::new(stalled_replica_config(peer.endpoint(), &markets, stall))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut standby, MARKET_A, "10", 1).await;
    await_metrics(&running, "the shadow to hold the market", |metrics| {
        metrics.stats.shadow_snapshots_applied == 1
    })
    .await;

    // The standby's heartbeat evidence is refreshed well after the publishing connection's,
    // so the withheld drain cycle below ends inside the window where the publishing
    // connection has expired and the standby has not. Without it both deadlines lapse
    // together, no standby is viable, and the switch under test never happens.
    tokio::time::sleep(Duration::from_millis(700)).await;
    standby.send_ping().await;
    standby.expect_pong(STEP_TIMEOUT).await;

    send_book(&mut primary, MARKET_A, "10", 1).await;
    let base = await_book(
        &mut book,
        "market A live on the publishing connection",
        is_live,
    )
    .await;
    assert_eq!(
        levels(&base, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "both connections delivered the same state, so the shadow agrees with the book"
    );

    // The accepted base above armed the withheld drain cycle: this frame reaches the ingest
    // queue and stays there until after the publishing connection's deadline has lapsed.
    tokio::time::sleep(Duration::from_millis(100)).await;
    send_book(&mut standby, MARKET_A, "30", 2).await;

    let refused = await_book_while_feeding(
        &mut standby,
        &mut book,
        "market A judged against the standby frame that was already queued",
        |published| !is_live(published),
    )
    .await;
    assert!(
        is_stale(&refused, AuthorityReason::ReplicaDivergence),
        "the queued standby frame disagreed with the published book, so the market's \
         authority could not cross the switch: {:?}",
        refused.authority()
    );
    assert_eq!(
        levels(&refused, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "a refused shadow is never installed over the book"
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.source_switches, 1,
        "the standby was still viable and took the publishing role"
    );
    assert_eq!(
        stats.markets_promoted, 0,
        "the market's drained shadow disagreed, so nothing promoted"
    );
    assert_eq!(
        stats.markets_promotion_refused, 1,
        "the market whose queued frame moved its shadow away was refused"
    );
}

/// A publishing connection that ended holding an undelivered loss cannot hand a market's
/// authority to a standby as though nothing was lost.
///
/// `NoticeUndeliverable` is the one end reason that hides a loss rather than reporting one:
/// the note the connection could not hand over may have been the overload report itself, or
/// a book update that never reached the book. Two replicas agreeing after that says only
/// that neither of them holds what went missing. Promoting on it would erase a known local
/// loss behind a takeover, and leave the book live on state no evidence covers.
#[tokio::test]
async fn a_publishing_connection_that_lost_notices_cannot_promote_its_loss_away() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(ShardConfig {
        ingest_capacity: 4,
        ingest_stall: Some(Duration::from_secs(4)),
        ..replica_config(peer.endpoint(), &markets)
    })
    .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut standby, MARKET_A, "10", 1).await;
    await_metrics(&running, "the shadow to hold the market", |metrics| {
        metrics.stats.shadow_snapshots_applied == 1
    })
    .await;
    send_book(&mut primary, MARKET_A, "10", 1).await;
    let base = await_book(&mut book, "market A live", is_live).await;

    // The accepted base armed the withheld drain cycle. Every frame below therefore competes
    // for a queue nothing is draining: the sink fills, starts dropping, and cannot hand over
    // the overload report it owes before the socket goes. The state is repeated rather than
    // moved so the shadow still agrees at the switch — what must refuse the promotion is the
    // loss, not a divergence.
    tokio::time::sleep(Duration::from_millis(100)).await;
    for version in 2..60 {
        send_book(&mut primary, MARKET_A, "10", version).await;
    }
    primary.drop_abruptly().await;

    let refused = await_book_while_feeding(
        &mut standby,
        &mut book,
        "market A refused against the publishing connection's undelivered loss",
        |published| !is_live(published),
    )
    .await;
    assert!(
        is_stale(&refused, AuthorityReason::Overload),
        "the loss the ended connection could not report is the market's own, and stays \
         distinguishable from a plain disconnect: {:?}",
        refused.authority()
    );
    assert_eq!(
        levels(&refused, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "a refused market keeps the last state the venue reported for it"
    );
    assert!(
        refused.revision() > base.revision(),
        "the loss was reported as a revision of its own rather than left implicit"
    );

    let stats = running.finish().await;
    assert!(
        stats.overload_drops > 0 || stats.markets_promotion_refused > 0,
        "the run observed the overload it was built around: {:?}",
        stats
    );
    assert_eq!(
        stats.markets_promoted, 0,
        "no market carried its authority across a switch away from a connection that lost \
         notices"
    );
    assert_eq!(
        stats.markets_promotion_refused, 1,
        "the market was refused, and its refusal counted"
    );
}

/// A promotion out of a standby role that still holds a fenced predecessor ends that
/// predecessor's socket, and leaves the vacated role on its own reconnect ladder.
///
/// Two hazards share one scene, because one line of code caused both: replacing the whole
/// slot on promotion. A dropped [`JoinHandle`] detaches its task rather than ending it, so
/// the fenced generation's socket becomes unreachable by any cleanup — a descriptor and a
/// venue connection nothing can close, accumulating one per promotion. The same replacement
/// discards the role's backoff state, so the vacancy is filled by the next dial pass instead
/// of by the ladder, and a flapping venue can spend the shared attempt budget as fast as it
/// can promote.
#[tokio::test]
async fn a_promotion_ends_the_vacated_role_s_fenced_socket_and_keeps_its_ladder() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let shard = Shard::new(ShardConfig {
        // Long enough that a linger expiry cannot be mistaken for the promotion ending the
        // fenced socket, and a ladder slow enough for the vacancy it leaves to be observable.
        fenced_linger: Duration::from_secs(30),
        initial_backoff: Duration::from_millis(1_200),
        max_backoff: Duration::from_secs(4),
        ..replica_config(peer.endpoint(), &[MARKET_A])
    })
    .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let handle = shard.handle();
    let running = Running::start(shard);
    let (primary, mut fenced_standby) = connect_pair(&mut peer, &running, &[MARKET_A]).await;
    drop(primary);

    // A set change retires every role's connection, which is what gives the standby role a
    // fenced predecessor while its replacement carries the new set.
    let outcomes = handle
        .add(vec![MARKET_B.to_owned()])
        .await
        .expect("the shard takes the add");
    assert_eq!(outcomes.len(), 1, "one market was named");
    let (mut replacement_primary, mut replacement_standby) =
        redialled_pair(&mut peer, &running, &[MARKET_A, MARKET_B]).await;

    send_book(&mut replacement_standby, MARKET_A, "10", 1).await;
    send_book(&mut replacement_primary, MARKET_A, "10", 1).await;
    await_book(&mut book, "market A live on the replacement", is_live).await;
    let promotable = await_status(&running, "the replacement shadow agreeing", |status| {
        standby_rows(status)
            .first()
            .is_some_and(|row| row.agreeing_markets == Some(1))
    })
    .await;
    let promoted_generation = standby_rows(&promotable)
        .first()
        .expect("a standby row is reported")
        .generation;

    replacement_primary.drop_abruptly().await;
    await_status(&running, "the replacement standby taking over", |status| {
        publishing_generation(status) == Some(promoted_generation)
    })
    .await;

    peer.expect_no_connection(Duration::from_millis(500)).await;
    assert!(
        fenced_standby
            .read_text_frame(Duration::from_secs(3))
            .await
            .is_none(),
        "the fenced predecessor's socket is still open, so nothing can close it: promotion \
         detached its task instead of ending it"
    );

    let mut refilled = peer.next_connection().await;
    assert_eq!(
        refilled.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the vacated standby role is refilled on its ladder rather than abandoned"
    );
    let _ = running.finish().await;
}

/// Why redundancy disappeared stays distinguishable after the standby that carried it is
/// gone.
///
/// A standby's end costs no book anything, so nothing about it reaches a market's
/// authority — and a shard that reported it only as an absent connection row would leave an
/// operator unable to tell a venue refusing the namespace from a socket that died. They are
/// different problems with different fixes, and the connection row that could have told them
/// apart is exactly what the failure removes.
#[tokio::test]
async fn a_standby_end_keeps_the_reason_it_ended_for() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let running = Running::start(shard);
    let (_primary, standby) = connect_pair(&mut peer, &running, &markets).await;

    standby.drop_abruptly().await;
    await_metrics(&running, "the dropped standby counted", |metrics| {
        metrics.stats.standby_ends.get("disconnect").copied() == Some(1)
    })
    .await;

    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&markets),
        "the standby role dialled a replacement"
    );
    replacement.send_raw("41/markets,").await;
    let metrics = await_metrics(&running, "the refused namespace counted", |metrics| {
        metrics.stats.standby_ends.get("protocol").copied() == Some(1)
    })
    .await;

    assert_eq!(
        metrics.stats.standby_ends.get("disconnect").copied(),
        Some(1),
        "the venue refusing the namespace did not overwrite the socket that died: {:?}",
        metrics.stats.standby_ends
    );
    let _ = running.finish().await;
}

/// A deliberate subscription replacement ends a standby's connection without counting as a
/// standby failure.
///
/// A desired-set change retires every role's connection by design, and the event is already
/// counted as a set replacement. Recording it again under a failure reason would increment
/// a disconnect counter on every ordinary reconciliation, and an operator alerting on
/// standby failures would page on routine market churn.
///
/// Every connection this scene opens stays open until the shard has stopped. A peer socket
/// dropped while the shard still runs is a standby failure the shard is right to count, and
/// counting it here would be indistinguishable from the replacement being miscounted — the
/// one thing this contract denies.
#[tokio::test]
async fn a_deliberate_replacement_is_not_a_standby_failure() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let running = Running::start(shard);
    let (_primary, _standby) = connect_pair(&mut peer, &running, &markets).await;

    let accepted = running
        .handle
        .add(owned(&[MARKET_B]))
        .await
        .expect("the shard is running");
    assert_eq!(accepted[0].status, MarketStatus::Accepted);
    let (_replacement_primary, _replacement_standby) =
        redialled_pair(&mut peer, &running, &[MARKET_A, MARKET_B]).await;
    let replaced = await_metrics(&running, "both roles' connections replaced", |metrics| {
        metrics.stats.set_replacements >= 2
    })
    .await;
    assert!(
        replaced.stats.standby_ends.is_empty(),
        "a deliberate replacement is a set replacement, not a standby failure: {:?}",
        replaced.stats.standby_ends
    );

    let stats = running.finish().await;
    assert!(
        stats.standby_ends.is_empty(),
        "the replacement stayed uncounted as a failure for the whole run: {:?}",
        stats.standby_ends
    );
    assert!(
        stats.set_replacements >= 1,
        "the replacement is still counted where it belongs"
    );
}

/// A shard running no standby has no standby ends to report, whatever else it survives.
#[tokio::test]
async fn a_shard_running_no_standby_reports_no_standby_ends() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(single_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let running = Running::start(shard);
    let mut primary = peer.next_connection().await;
    assert_eq!(primary.complete_handshake().await.slugs, owned(&markets));
    primary.drop_abruptly().await;
    let mut replacement = peer.next_connection().await;
    assert_eq!(
        replacement.complete_handshake().await.slugs,
        owned(&markets)
    );

    let stats = running.finish().await;
    assert!(
        stats.standby_ends.is_empty(),
        "a publishing connection's end is not a standby's: {:?}",
        stats.standby_ends
    );
}

/// Reported promotion coverage counts only markets a takeover would actually carry.
///
/// A standby's shadow can hold levels identical to a published book that has already lost
/// authority: the loss was the publishing rail's, and it moved no level. Counting that
/// market as covered would tell an operator redundancy is in place for a market the
/// takeover is required to refuse — a book that has lost authority must recover from a
/// fresh venue base, never from a source switch. Coverage and eligibility answer to one
/// predicate so the figure cannot drift from the decision.
#[tokio::test]
async fn coverage_counts_no_market_a_takeover_would_refuse() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let markets = [MARKET_A];
    let shard = Shard::new(replica_config(peer.endpoint(), &markets))
        .expect("test shard configuration is valid");
    let mut book = shard.observe(MARKET_A).expect("market A is in the set");
    let running = Running::start(shard);
    let (mut primary, mut standby) = connect_pair(&mut peer, &running, &markets).await;

    send_book(&mut standby, MARKET_A, "10", 1).await;
    send_book(&mut primary, MARKET_A, "10", 1).await;
    let base = await_book(&mut book, "market A live", is_live).await;
    await_status(&running, "the shadow agreeing with the book", |status| {
        status.standby_agreeing_markets == 1
    })
    .await;

    // An undecodable frame the venue named `orderbookUpdate` is a loss of the publishing
    // rail: it costs the book its authority and moves no level, so the standby's shadow goes
    // on holding exactly what the book still shows.
    primary
        .send_raw("42/markets,[\"orderbookUpdate\",{\"marketSlug\":17}]")
        .await;
    let stale = await_book(&mut book, "the publishing rail's loss", |published| {
        !is_live(published)
    })
    .await;
    assert!(
        is_stale(&stale, AuthorityReason::LocalLoss),
        "the undecodable frame cost the rail's books their authority: {:?}",
        stale.authority()
    );
    assert_eq!(
        levels(&stale, Side::Bid),
        expect_levels(&[("0.51", "10")]),
        "the loss moved no level, so the shadow still matches what the book shows"
    );
    assert!(stale.revision() > base.revision());

    let status = await_status(
        &running,
        "coverage answering for the stale book",
        |status| status.standby_agreeing_markets == 0,
    )
    .await;
    assert_eq!(
        standby_rows(&status)
            .first()
            .and_then(|row| row.agreeing_markets),
        Some(0),
        "the connection row agrees with the shard-wide figure"
    );
    assert_eq!(
        running.metrics().await.standby_agreeing_markets,
        0,
        "a scrape and a status page cannot name different coverage"
    );

    // The takeover is the thing the figure claimed to predict, so it has to agree with it.
    primary.drop_abruptly().await;
    let refused = await_book_while_feeding(
        &mut standby,
        &mut book,
        "the market's answer to the switch",
        |published| published.revision() > stale.revision(),
    )
    .await;
    assert!(
        !is_live(&refused),
        "a book that had already lost authority cannot recover through a source switch: {:?}",
        refused.authority()
    );

    let stats = running.finish().await;
    assert_eq!(
        stats.markets_promoted, 0,
        "the takeover carried exactly the coverage that was reported: none"
    );
}
