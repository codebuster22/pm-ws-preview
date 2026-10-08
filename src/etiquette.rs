//! Process-wide venue etiquette: the connection-attempt budget and the command pacer.
//!
//! Two limits, both enforced for the whole process rather than per connection, because
//! every rail in this process dials from one source address and a venue that tracks
//! traffic by source address sees the sum:
//!
//! - **Attempts.** [`admit_attempt`] spends one connection attempt against a rolling
//!   24-hour window, up to the budget its caller names. [`DAILY_ATTEMPT_BUDGET`] is this
//!   project's own default of 280 attempts per day; no retrieved venue documentation
//!   places a ceiling. A refusal names the instant the oldest attempt inside the window
//!   ages out, so a caller waits rather than spending past the budget. The window rolls
//!   with the clock instead of resetting on a calendar boundary, and the ledger's storage
//!   is bounded by the budget it enforces. It is process-local: it re-arms empty on
//!   restart.
//! - **Commands.** [`reserve_command_grant`] hands out the next slot for a
//!   subscription-bearing command toward one venue endpoint, spaced at least the caller's
//!   configured interval behind the slot before it. [`MIN_COMMAND_INTERVAL`] is this
//!   project's own default floor of one such command per 500 ms per endpoint. Reserving
//!   never blocks and never refuses: it answers an [`Instant`], and waiting for it is the
//!   caller's job. Slots are keyed by endpoint and bounded at [`MAX_PACED_ENDPOINTS`];
//!   past that, further endpoints share one overflow slot, which is stricter than
//!   per-endpoint pacing and never looser.
//!
//! Both are best spent at the write rather than at the decision that authorized it: a dial
//! and a handshake stand between the two, so metering decision points lets two connections
//! whose handshakes converge land inside the floor.

use core::time::Duration;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use tokio::time::Instant;

const ONE_DAY: Duration = Duration::from_secs(86_400);

/// The default number of connection attempts this process will spend in a day, when a
/// configuration does not name its own.
///
/// No retrieved venue documentation places a daily connection-attempt ceiling; two hundred
/// eighty is this project's own conservative default. The ledger enforcing whatever budget
/// is configured is process-local and bounds one process lifetime: it re-arms empty on
/// restart, and a second process on the same host keeps its own.
pub const DAILY_ATTEMPT_BUDGET: u64 = 280;

/// The default shortest gap between two subscription-bearing commands this process puts on
/// the wire toward one endpoint, when a configuration does not name its own.
///
/// No retrieved venue documentation places a sustained-command ceiling; this is this
/// project's own conservative default for a polite wire citizen. Every such command draws
/// from one permit: every rail's establishing subscription, and every later re-emit.
/// Pacing is a property of one endpoint and the source address dialling it, not of one
/// socket, so pacing each connection separately would let two connections exceed the
/// configured floor while each stayed inside it — which is why [`COMMAND_PACERS`] is a
/// process-wide static keyed by endpoint.
///
/// The permit is taken inside the connection writer, immediately before the bytes of a
/// subscription command are written, rather than where the rail decides to send one. The
/// distinction matters because a dial and a handshake stand between a decision and the
/// write, and neither is timed by the code that authorized the command: metering decision
/// points leaves two connections whose handshakes converge free to land their subscriptions
/// inside the floor. Metering the write makes the spacing on the wire the spacing the
/// configured floor names.
///
/// A window that must cover a command still has to allow for that wait, which is why a
/// reissue's deadline is the configured window plus this floor.
pub const MIN_COMMAND_INTERVAL: Duration = Duration::from_millis(500);

/// Every connection attempt this process has made inside the rolling day, whatever the
/// caller's configured budget admitted it.
///
/// Process-wide because every role, every market, and every ladder in this process dials
/// from one source address, and a ledger enforced per ladder would not see what a venue
/// that does track attempts by source address would see.
static ATTEMPT_LEDGER: Mutex<AttemptLedger> = Mutex::new(AttemptLedger::new());

/// A rolling 24-hour record of connection attempts spent against a caller's configured
/// daily budget.
///
/// Storage is bounded by the budget it is asked to enforce: an attempt is recorded only
/// when it is admitted, admission stops at the budget, and entries older than a day are
/// discarded on every decision, so the window rolls rather than resetting on a calendar
/// boundary.
struct AttemptLedger {
    spent: VecDeque<Instant>,
}

impl AttemptLedger {
    const fn new() -> Self {
        Self {
            spent: VecDeque::new(),
        }
    }

    /// Records one connection attempt made at `now`, or refuses it and names the instant
    /// the oldest attempt still inside the rolling day ages out of it.
    ///
    /// A refusal is a clamp, not a counter that lies: the caller waits for the returned
    /// instant instead of spending an attempt past `budget`.
    fn admit(&mut self, now: Instant, budget: u64) -> Result<(), Instant> {
        while self
            .spent
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= ONE_DAY)
        {
            let _ = self.spent.pop_front();
        }
        let budget = usize::try_from(budget).unwrap_or(usize::MAX);
        if self.spent.len() >= budget {
            let oldest = self.spent.front().copied().unwrap_or(now);
            return Err(oldest + ONE_DAY);
        }
        self.spent.push_back(now);
        Ok(())
    }
}

/// Spends one attempt from the process-wide ledger against the caller's configured
/// `budget`, or names when the next one is free.
pub(crate) fn admit_attempt(now: Instant, budget: u64) -> Result<(), Instant> {
    ATTEMPT_LEDGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .admit(now, budget)
}

/// The instant a subscription-bearing command may follow the one reserved at `last`, spaced
/// at least `interval` after it.
pub(crate) fn command_grant_at(last: Option<Instant>, now: Instant, interval: Duration) -> Instant {
    last.map_or(now, |at| at + interval).max(now)
}

/// The furthest instant any subscription-bearing command has been reserved for against each
/// venue endpoint this process has dialled.
///
/// Process-wide for the same reason [`ATTEMPT_LEDGER`] is: every rail and socket in this
/// process dials from one source address, so a pacer owned by one connection would let two
/// connections each stay inside their own configured floor while the address they share
/// crosses it.
///
/// The key is the endpoint because pacing is a property of traffic *to one address*.
/// Commands this process sends somewhere else — a second venue, a controlled peer in a
/// fault test — dial a different address, and pacing them against this floor would throttle
/// work the configured floor was never meant to cover.
///
/// A slot spaces each caller by *that caller's own* configured floor against the shared
/// reservation, so a process running deliberately different floors toward one endpoint gets
/// each caller's own promise kept rather than the strictest of them imposed on all: a caller
/// configured at 500 ms never writes within 500 ms of the previous command, while a caller
/// configured at zero may write immediately after it.
///
/// The instant held is a *reservation* rather than a record of the last write: it is where
/// the schedule has been filled to, which is what lets a caller learn its own position in
/// the queue at the moment it joins one. See [`reserve_command_grant`].
///
/// Storage is bounded at [`MAX_PACED_ENDPOINTS`]. A process that dials more endpoints than
/// that paces every further one against a single shared slot, which is stricter than
/// per-endpoint pacing and never looser.
static COMMAND_PACERS: Mutex<BTreeMap<String, Instant>> = Mutex::new(BTreeMap::new());

/// The most venue endpoints [`COMMAND_PACERS`] tracks separately.
const MAX_PACED_ENDPOINTS: usize = 64;

/// The pacer slot an endpoint uses: its own while there is room, and one shared overflow
/// slot past that.
fn pacer_slot(pacers: &BTreeMap<String, Instant>, endpoint: &str) -> String {
    if pacers.contains_key(endpoint) || pacers.len() < MAX_PACED_ENDPOINTS {
        endpoint.to_owned()
    } else {
        String::new()
    }
}

/// Reserves the next subscription-bearing command slot against `endpoint` and names the
/// instant its bytes may be written.
///
/// The grant is `max(last_reserved + interval, now)`, and `last_reserved` advances to it, so
/// the caller after this one stands behind the instant this one *holds* rather than behind
/// the instant it asked. Reserving never refuses and never blocks: it hands back a place in
/// a queue, and waiting for that place is the caller's job.
///
/// This is the whole point of a reservation rather than a permit. The pacer is process-wide,
/// so a fleet whose connections all need a command at once forms one queue, and the `k`-th
/// command cannot reach the wire before `k` intervals have passed. A caller that only
/// learned "not yet, try again" could never tell how long its own wait was, and every
/// deadline written against it had to guess — invariably at one interval, which is right
/// only for the caller that happens to stand first. A caller that learns its granted instant
/// can budget for the queue it is actually in.
///
/// Reserving and spending are one locked operation, so two callers cannot be handed the same
/// place: whoever takes the lock second is spaced behind whoever took it first.
///
/// A configured interval of zero degenerates to `max(last_reserved, now)` — every caller is
/// granted the present moment and nothing waits. No special case.
///
/// A reservation whose command is never written leaves a hole in the schedule: the
/// connection's task ended before it could take the command, or the encoded command turned
/// out not to fit, and the slot passes unused while every later reservation still stands
/// behind it. That is accepted and never compensated for. A hole can only make this process
/// quieter toward the venue than its configured floor allows, which is the safe direction,
/// and any machinery that reclaimed one — a released reservation, a compacted schedule —
/// would have to be correct under exactly the races the reservation exists to remove.
///
/// Storage is bounded at [`MAX_PACED_ENDPOINTS`]; the overflow slot is shared, which is
/// stricter than per-endpoint pacing and never looser.
pub(crate) fn reserve_command_grant(endpoint: &str, now: Instant, interval: Duration) -> Instant {
    let mut pacers = COMMAND_PACERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let slot = pacer_slot(&pacers, endpoint);
    let granted = command_grant_at(pacers.get(&slot).copied(), now, interval);
    let _ = pacers.insert(slot, granted);
    granted
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The most frequent a reconnect cycle can recur under the rail's conservative ladder:
    /// a generation that has to live one stability window (60 s) before its backoff resets,
    /// plus the shortest a jittered one-second initial backoff can return (750 ms). It is
    /// therefore the worst case this test drives the ledger with.
    const WORST_CASE_CYCLE: Duration = Duration::from_millis(60_750);

    fn test_endpoint(label: &str) -> String {
        format!("ws://127.0.0.1/{label}")
    }

    #[test]
    fn the_attempt_ledger_clamps_a_stable_at_threshold_flap_across_both_roles() {
        let start = Instant::now();
        let mut ledger = AttemptLedger::new();
        let mut admitted = 0u64;
        let mut refused = 0u64;
        let mut elapsed = Duration::ZERO;
        while elapsed < ONE_DAY {
            for _role in 0..2u32 {
                match ledger.admit(start + elapsed, DAILY_ATTEMPT_BUDGET) {
                    Ok(()) => admitted += 1,
                    Err(free_at) => {
                        refused += 1;
                        assert!(
                            free_at > start + elapsed,
                            "a refusal names a future instant to wait for"
                        );
                    }
                }
            }
            elapsed += WORST_CASE_CYCLE;
        }
        assert_eq!(
            admitted, DAILY_ATTEMPT_BUDGET,
            "both roles flapping at the stability threshold spend the budget and no more"
        );
        assert!(refused > 0, "the walk must reach the clamp to prove it");
        assert!(
            ledger.spent.len() <= usize::try_from(DAILY_ATTEMPT_BUDGET).unwrap_or(usize::MAX),
            "the ledger's storage is bounded by the budget it enforces"
        );
        let aged_out = ledger
            .admit(start + ONE_DAY + WORST_CASE_CYCLE, DAILY_ATTEMPT_BUDGET)
            .is_ok();
        assert!(
            aged_out,
            "the window rolls: an attempt older than a day frees a slot"
        );
    }

    #[test]
    fn command_grants_are_never_closer_than_the_configured_floor() {
        let now = Instant::now();
        assert_eq!(
            command_grant_at(None, now, MIN_COMMAND_INTERVAL),
            now,
            "the first command waits for nothing"
        );
        let granted = command_grant_at(Some(now), now, MIN_COMMAND_INTERVAL);
        assert_eq!(granted, now + MIN_COMMAND_INTERVAL);
        assert_eq!(
            command_grant_at(
                Some(now),
                now + MIN_COMMAND_INTERVAL * 2,
                MIN_COMMAND_INTERVAL
            ),
            now + MIN_COMMAND_INTERVAL * 2,
            "a permit already free grants at once rather than in the past"
        );
    }

    /// A configured floor of zero is a valid operator choice meaning no pacing at all: the
    /// grant arithmetic (`max(last, now)`) already handles it with no special case, so a
    /// caller placing back-to-back commands is granted immediately rather than refused or
    /// stalled.
    #[test]
    fn a_zero_configured_floor_grants_every_command_at_once() {
        let now = Instant::now();
        assert_eq!(
            command_grant_at(Some(now), now, Duration::ZERO),
            now,
            "no pacing means no wait, not a refusal"
        );
    }

    /// The reservation ledger hands out places in a queue rather than permits to write now.
    ///
    /// Three properties, in the order they matter: consecutive callers are spaced by the
    /// floor whatever instant each of them asked at, a caller arriving after the queue has
    /// drained gets the present moment rather than a stale one, and a floor of zero produces
    /// no queue at all.
    ///
    /// The hole is the fourth reservation here. The ledger has no route by which a command
    /// that was reserved and then never written can give its place back — nothing reports a
    /// write, and nothing releases a slot — so the caller behind it still stands a full floor
    /// behind a slot that was never used. That leaves this process quieter toward the venue
    /// than its configured floor allows, which is the direction a pacer is allowed to err in.
    #[test]
    fn reservations_hand_out_places_in_one_queue_and_never_reclaim_an_unused_place() {
        const INTERVAL: Duration = Duration::from_millis(100);
        let paced = test_endpoint("reservations-hand-out-places");
        let now = Instant::now();

        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now,
            "an endpoint with an empty queue grants the present moment"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL,
            "the second caller stands behind the place the first holds"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL * 2,
            "spacing follows the places already handed out, not the instant a caller asked"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL * 3,
            "the place before this one was never written, and is not reclaimed"
        );
        assert_eq!(
            reserve_command_grant(&paced, now + INTERVAL * 20, INTERVAL),
            now + INTERVAL * 20,
            "a queue that has drained grants the present moment rather than a stale place"
        );

        let unpaced = test_endpoint("reservations-with-no-floor");
        assert_eq!(reserve_command_grant(&unpaced, now, Duration::ZERO), now);
        assert_eq!(
            reserve_command_grant(&unpaced, now, Duration::ZERO),
            now,
            "a floor of zero is no queue: every caller is granted the present moment"
        );
    }
}
