//! The platform wait and wake primitives one 4-byte doorbell cell is parked on.
//!
//! A consumer that has caught up must either burn a core spinning or sleep on a timer; the
//! doorbell is what lets it sleep with no timer at all. It is a wake-only hint — nothing is
//! ever decoded from its value — so the only two operations it needs are "block while this
//! cell still reads `expected`" and "release everyone blocked on this cell".
//!
//! Both platforms compare the cell inside the kernel, so both need an *address* rather than
//! a value; [`super::cell::WakeAddress`] is the whole of what crosses out of the cell layer,
//! and it carries no load, no store and no dereference. The primitives themselves are
//! declared here by hand rather than pulled from a binding crate: the daemon takes no
//! dependency it does not need, and the same hand-rolled style already carries the napi
//! surface in `src/ffi`.
//!
//! **Linux** uses `futex(2)` `FUTEX_WAIT` / `FUTEX_WAKE` *without* `FUTEX_PRIVATE_FLAG`,
//! because the writer and its consumers are different processes sharing one file mapping.
//! **macOS** uses `os_sync_wait_on_address` and `os_sync_wake_by_address_all` with the
//! `OS_SYNC_*_SHARED` flags, available since macOS 14.4 / iOS 17.4; a 4-byte size is one of
//! the two the API accepts, which is why the doorbell is a `u32` rather than sharing the
//! 64-bit publication generation.
#![allow(unsafe_code)]

use super::cell::WakeAddress;
use core::time::Duration;
use std::time::Instant;

/// How a wait ended.
///
/// [`Self::Woken`] and [`Self::ValueMismatch`] are not distinguishable on every platform —
/// Darwin reports both as one success return — so a caller must treat them identically and
/// re-read the cell it waited on. The distinction is reported where the platform makes it
/// (Linux answers `EAGAIN` for a mismatch) and is diagnostic, never a branch a protocol may
/// depend on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaitOutcome {
    /// The kernel released this waiter: a wake, or a spurious wake the platform allows.
    Woken,
    /// The wait's own deadline expired before any wake arrived.
    TimedOut,
    /// The cell did not hold the expected value, so the wait never blocked.
    #[cfg_attr(
        not(target_os = "linux"),
        expect(
            dead_code,
            reason = "Darwin reports a mismatch through the same success return as a wake"
        )
    )]
    ValueMismatch,
}

/// How a wake ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WakeOutcome {
    /// The platform released the waiters it found. The count is what the platform reports,
    /// and is 0 on platforms that report none.
    Woken(u32),
    /// Nobody was waiting. The ordinary case for a segment with no parked consumer, and
    /// never an error: the writer wakes unconditionally because a read-only consumer owns no
    /// cell it could register a waiter count in.
    NoWaiters,
}

/// A platform wait or wake that failed for a reason this build does not treat as ordinary,
/// carrying the raw platform error number.
///
/// The number is kept rather than mapped so a fault that only appears on one host can be
/// named exactly in a diagnostic instead of being flattened into "failed".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DoorbellFault {
    errno: i32,
}

impl core::fmt::Display for DoorbellFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "doorbell primitive failed: errno {}", self.errno)
    }
}
impl std::error::Error for DoorbellFault {}
impl DoorbellFault {
    /// The raw platform error number this fault carries.
    ///
    /// Exposed so a caller above this module can surface a typed fault of its own — the
    /// consumer wait path reports it rather than only this type's formatted `Display`.
    pub(super) fn errno(self) -> i32 {
        self.errno
    }
}

/// How many transient platform faults one operational wait absorbs before surfacing one.
///
/// A total per call, not a consecutive run: the budget is what bounds the whole call, and a
/// fault every other attempt is exactly as much a stuck address as a fault every attempt.
/// Past the budget the fault is surfaced rather than retried forever, so a genuinely
/// unwaitable address is a typed answer and never a silent spin.
const TRANSIENT_FAULT_RETRIES: u32 = 8;

/// The pause between two attempts separated by a transient fault.
///
/// Short enough to be invisible against the millisecond-scale timeouts a consumer parks with,
/// long enough that eight of them do not re-enter the kernel inside the same scheduling
/// quantum that faulted. Clamped to whatever remains of the caller's own deadline, so a
/// backoff can never outlive the wait it belongs to.
const TRANSIENT_FAULT_BACKOFF: Duration = Duration::from_micros(50);

/// Whether this platform reports a transient condition through the same error number it
/// reports a permanently unwaitable address with.
///
/// Darwin documents `EFAULT` from `os_sync_wait_on_address` as either an address the kernel
/// cannot reach *or* a transient failure to fault the page in under memory pressure, and only
/// the first is permanent. Every other platform in this build answers `EFAULT` for the
/// address alone, where a retry would be a loop around a condition that cannot change.
const TRANSIENT_FAULTS_ARE_RETRYABLE: bool = cfg!(target_os = "macos");

/// What a failed platform wait should do next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultResponse {
    /// Not a failure at all: retry at once, bounded only by the caller's own deadline. A
    /// signal and the documented low-memory bail-out are both of this kind.
    RetryNow,
    /// Retry after [`TRANSIENT_FAULT_BACKOFF`], and only while the call's transient-fault
    /// budget lasts.
    RetryAfterBackoff,
    /// Surface as a typed fault.
    Surface,
}

/// Classifies `errno` for a call that has already spent `transient_faults` of its budget.
///
/// `faults_are_transient` is the platform question of [`TRANSIENT_FAULTS_ARE_RETRYABLE`],
/// passed in rather than read from the constant so that both answers are reachable in one
/// test run on any host — the classification is the seam a test can drive, because the
/// platform call itself cannot be made to fault on demand.
///
/// It is also what separates the two kinds of wait this module serves. An operational park
/// has already been told by the segment's feature bit that this address is waitable, so the
/// transient reading of `EFAULT` is the live one and absorbing a bounded number of them is
/// what keeps one moment of memory pressure from permanently downgrading a valid parked
/// consumer to a typed I/O failure. The creation-time probe of [`waiting_is_supported`] is
/// asking the opposite question — is this address waitable at all — and passes `false`, so
/// the first `EFAULT` decides the segment's placement exactly as it always has.
fn respond_to(errno: i32, faults_are_transient: bool, transient_faults: u32) -> FaultResponse {
    if errno == platform::EINTR || errno == platform::ENOMEM {
        return FaultResponse::RetryNow;
    }
    if errno == platform::EFAULT && faults_are_transient {
        if transient_faults < TRANSIENT_FAULT_RETRIES {
            return FaultResponse::RetryAfterBackoff;
        }
        return FaultResponse::Surface;
    }
    FaultResponse::Surface
}

/// Runs `attempt` against the deadline `timeout` fixes, applying [`respond_to`]'s policy to
/// every failure, and hands `attempt` whatever remains of that deadline each time.
///
/// Split out from [`wait`] so the policy can be driven with injected outcomes: a test can
/// hand this a closure that fails on demand, which no real platform wait can be made to do.
/// A zero or already-expired timeout returns [`WaitOutcome::TimedOut`] without calling
/// `attempt` at all, because both platforms refuse a zero timeout argument.
fn wait_retrying(
    timeout: Option<Duration>,
    faults_are_transient: bool,
    mut attempt: impl FnMut(Option<Duration>) -> Result<WaitOutcome, DoorbellFault>,
) -> Result<WaitOutcome, DoorbellFault> {
    let deadline = timeout.map(|timeout| Instant::now() + timeout);
    let remaining = |deadline: Option<Instant>| match deadline {
        None => Some(None),
        Some(deadline) => {
            let now = Instant::now();
            (now < deadline).then(|| Some(deadline - now))
        }
    };
    let mut transient_faults = 0_u32;
    loop {
        let Some(left) = remaining(deadline) else {
            return Ok(WaitOutcome::TimedOut);
        };
        let fault = match attempt(left) {
            Ok(outcome) => return Ok(outcome),
            Err(fault) => fault,
        };
        match respond_to(fault.errno, faults_are_transient, transient_faults) {
            FaultResponse::RetryNow => {}
            FaultResponse::RetryAfterBackoff => {
                transient_faults += 1;
                let Some(left) = remaining(deadline) else {
                    return Ok(WaitOutcome::TimedOut);
                };
                std::thread::sleep(match left {
                    None => TRANSIENT_FAULT_BACKOFF,
                    Some(left) => TRANSIENT_FAULT_BACKOFF.min(left),
                });
            }
            FaultResponse::Surface => return Err(fault),
        }
    }
}

/// Blocks while the cell at `address` still reads `expected`, for at most `timeout`.
///
/// `timeout` of `None` parks indefinitely. Interruptions and the transient early returns both
/// platforms document — a signal, a low-memory bail-out, and on Darwin an `EFAULT` raised by
/// memory pressure rather than by the address — are retried inside this call against the
/// deadline `timeout` fixes, so a caller sees a spurious wake only as [`WaitOutcome::Woken`]
/// and never as an error. [`respond_to`] carries which failures those are and what bounds
/// them. A zero or already-expired timeout returns [`WaitOutcome::TimedOut`] without a
/// syscall, because both platforms refuse a zero timeout argument.
///
/// Fails with [`DoorbellFault`] carrying the platform error number for everything else, and
/// for a retryable failure that outlasts its own budget.
///
/// # Safety obligation of the caller
///
/// `address` must name a live 4-byte cell for the whole call. Every address this module is
/// handed comes from a [`super::cell::SegmentRegion`] the caller keeps mapped across the
/// wait; nothing here can enforce that, and it is the one precondition the type system does
/// not carry.
pub(super) fn wait(
    address: WakeAddress,
    expected: u32,
    timeout: Option<Duration>,
) -> Result<WaitOutcome, DoorbellFault> {
    wait_retrying(timeout, TRANSIENT_FAULTS_ARE_RETRYABLE, |remaining| {
        platform::wait(address, expected, remaining)
    })
}

/// Releases every waiter parked on the cell at `address`.
///
/// Never blocks, never sleeps and never retries: this runs on the publication path, so its
/// cost is one bounded syscall whatever the outcome. [`WakeOutcome::NoWaiters`] is the
/// ordinary answer when no consumer is parked and is not an error.
pub(super) fn wake_all(address: WakeAddress) -> Result<WakeOutcome, DoorbellFault> {
    platform::wake_all(address)
}

/// Whether this platform will wait on the cell at `address` at all.
///
/// Probes with a value the cell cannot be holding — the observed value plus one — so the wait
/// can never block: a platform that supports the mapping returns immediately with a mismatch,
/// and one that does not answers with an error. A `false` therefore means "wait somewhere
/// else", which is the whole question the segment's doorbell feature bit records.
///
/// The failure direction is deliberately fail-safe, which is why this probes rather than
/// calling [`wait`]. Darwin documents `EFAULT` as either an invalid address *or* a transient
/// low-memory condition; an operational park reads that ambiguity the retryable way, but a
/// probe reads it the pessimistic way and answers `false` on the first one. The pessimistic
/// answer costs a sibling page that always works, while an optimistic one would leave
/// consumers unable to park at all — so this never retries a fault, and a `false` here may
/// simply mean the probe ran under memory pressure.
pub(super) fn waiting_is_supported(address: WakeAddress, observed: u32) -> bool {
    let expected = observed.wrapping_add(1);
    wait_retrying(None, false, |remaining| {
        platform::wait(address, expected, remaining)
    })
    .is_ok()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{DoorbellFault, WaitOutcome, WakeAddress, WakeOutcome};
    use core::ffi::{c_int, c_long, c_void};
    use core::time::Duration;

    pub(super) const EINTR: i32 = 4;
    pub(super) const ENOMEM: i32 = 12;
    pub(super) const EFAULT: i32 = 14;
    const EAGAIN: i32 = 11;
    const ETIMEDOUT: i32 = 110;

    #[cfg(target_arch = "x86_64")]
    const SYS_FUTEX: c_long = 202;
    #[cfg(target_arch = "aarch64")]
    const SYS_FUTEX: c_long = 98;

    const FUTEX_WAIT: c_int = 0;
    const FUTEX_WAKE: c_int = 1;

    #[repr(C)]
    struct Timespec {
        seconds: i64,
        nanoseconds: i64,
    }

    unsafe extern "C" {
        fn syscall(number: c_long, ...) -> c_long;
    }

    fn fault() -> DoorbellFault {
        DoorbellFault {
            errno: std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default(),
        }
    }

    pub(super) fn wait(
        address: WakeAddress,
        expected: u32,
        remaining: Option<Duration>,
    ) -> Result<WaitOutcome, DoorbellFault> {
        let timeout = remaining.map(|remaining| Timespec {
            seconds: i64::try_from(remaining.as_secs()).unwrap_or(i64::MAX),
            nanoseconds: i64::from(remaining.subsec_nanos()),
        });
        let pointer = timeout
            .as_ref()
            .map_or(core::ptr::null(), core::ptr::from_ref)
            .cast::<c_void>();
        // SAFETY: `address` names a live 4-byte cell for the duration of this call, which is
        // the caller's stated obligation on `super::wait`. The kernel only reads that cell.
        // `timeout` outlives the call because it is a local, and a null pointer is the
        // documented way to say "no timeout". The variadic arguments match `futex(2)`'s
        // signature exactly: `uaddr, op, val, timeout, uaddr2, val3`.
        let answer = unsafe {
            syscall(
                SYS_FUTEX,
                address.as_ptr(),
                FUTEX_WAIT,
                expected,
                pointer,
                core::ptr::null::<c_void>(),
                0_u32,
            )
        };
        if answer == 0 {
            return Ok(WaitOutcome::Woken);
        }
        let fault = fault();
        match fault.errno {
            EAGAIN => Ok(WaitOutcome::ValueMismatch),
            ETIMEDOUT => Ok(WaitOutcome::TimedOut),
            _ => Err(fault),
        }
    }

    pub(super) fn wake_all(address: WakeAddress) -> Result<WakeOutcome, DoorbellFault> {
        // SAFETY: as for `wait` above; `FUTEX_WAKE` reads no user memory beyond the cell's
        // address and takes no timeout, so the two unused arguments are null and zero.
        let answer = unsafe {
            syscall(
                SYS_FUTEX,
                address.as_ptr(),
                FUTEX_WAKE,
                i32::MAX,
                core::ptr::null::<c_void>(),
                core::ptr::null::<c_void>(),
                0_u32,
            )
        };
        match answer {
            0 => Ok(WakeOutcome::NoWaiters),
            woken if woken > 0 => Ok(WakeOutcome::Woken(u32::try_from(woken).unwrap_or(u32::MAX))),
            _ => Err(fault()),
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{DoorbellFault, WaitOutcome, WakeAddress, WakeOutcome};
    use core::ffi::{c_int, c_void};
    use core::time::Duration;

    pub(super) const EINTR: i32 = 4;
    pub(super) const ENOMEM: i32 = 12;
    pub(super) const EFAULT: i32 = 14;
    const ENOENT: i32 = 2;
    const ETIMEDOUT: i32 = 60;

    const OS_SYNC_WAIT_ON_ADDRESS_SHARED: u32 = 1;
    const OS_SYNC_WAKE_BY_ADDRESS_SHARED: u32 = 1;
    const OS_CLOCK_MACH_ABSOLUTE_TIME: u32 = 32;
    const CELL_BYTES: usize = 4;

    unsafe extern "C" {
        fn os_sync_wait_on_address(
            address: *mut c_void,
            value: u64,
            size: usize,
            flags: u32,
        ) -> c_int;
        fn os_sync_wait_on_address_with_timeout(
            address: *mut c_void,
            value: u64,
            size: usize,
            flags: u32,
            clock: u32,
            timeout_nanos: u64,
        ) -> c_int;
        fn os_sync_wake_by_address_all(address: *mut c_void, size: usize, flags: u32) -> c_int;
    }

    fn fault() -> DoorbellFault {
        DoorbellFault {
            errno: std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or_default(),
        }
    }

    /// The timeout argument is nanoseconds, measured on this host: a requested 200 ms
    /// returned in 202 ms and a requested 50 ms in 52 ms. The clock identifier names which
    /// clock the *relative* timeout is measured against and does not change its unit, so no
    /// `mach_timebase_info` conversion belongs here — one would make every timeout roughly
    /// forty times short on an Apple-silicon timebase.
    pub(super) fn wait(
        address: WakeAddress,
        expected: u32,
        remaining: Option<Duration>,
    ) -> Result<WaitOutcome, DoorbellFault> {
        let cell = address.as_ptr().cast::<c_void>();
        let value = u64::from(expected);
        // SAFETY: `address` names a live, 4-byte-aligned cell for the duration of this call,
        // which is the caller's stated obligation on `super::wait`; the kernel only reads it.
        // The size and the shared flag match what every wake on this cell passes, which the
        // API requires. A zero timeout is refused by the platform and is excluded by
        // `super::wait` before it reaches here.
        let answer = unsafe {
            match remaining {
                None => {
                    os_sync_wait_on_address(cell, value, CELL_BYTES, OS_SYNC_WAIT_ON_ADDRESS_SHARED)
                }
                Some(remaining) => os_sync_wait_on_address_with_timeout(
                    cell,
                    value,
                    CELL_BYTES,
                    OS_SYNC_WAIT_ON_ADDRESS_SHARED,
                    OS_CLOCK_MACH_ABSOLUTE_TIME,
                    u64::try_from(remaining.as_nanos())
                        .unwrap_or(u64::MAX)
                        .max(1),
                ),
            }
        };
        if answer >= 0 {
            return Ok(WaitOutcome::Woken);
        }
        let fault = fault();
        match fault.errno {
            ETIMEDOUT => Ok(WaitOutcome::TimedOut),
            _ => Err(fault),
        }
    }

    pub(super) fn wake_all(address: WakeAddress) -> Result<WakeOutcome, DoorbellFault> {
        // SAFETY: as for `wait` above. The size and shared flag match every wait on this
        // cell, which the API requires for a waiter to be found.
        let answer = unsafe {
            os_sync_wake_by_address_all(
                address.as_ptr().cast::<c_void>(),
                CELL_BYTES,
                OS_SYNC_WAKE_BY_ADDRESS_SHARED,
            )
        };
        if answer >= 0 {
            return Ok(WakeOutcome::Woken(0));
        }
        let fault = fault();
        match fault.errno {
            ENOENT => Ok(WakeOutcome::NoWaiters),
            _ => Err(fault),
        }
    }
}

/// The seam a platform without a wait primitive in this build lands on.
///
/// An unimplemented platform (Windows's `WaitOnAddress` among them) fails every wait and
/// every wake with `ENOSYS`, which makes the segment take the sibling doorbell page and
/// makes a consumer's park a typed refusal rather than a silent hang.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::{DoorbellFault, WaitOutcome, WakeAddress, WakeOutcome};
    use core::time::Duration;

    pub(super) const EINTR: i32 = 4;
    pub(super) const ENOMEM: i32 = 12;
    pub(super) const EFAULT: i32 = 14;
    const ENOSYS: i32 = 78;

    pub(super) fn wait(
        _address: WakeAddress,
        _expected: u32,
        _remaining: Option<Duration>,
    ) -> Result<WaitOutcome, DoorbellFault> {
        Err(DoorbellFault { errno: ENOSYS })
    }

    pub(super) fn wake_all(_address: WakeAddress) -> Result<WakeOutcome, DoorbellFault> {
        Err(DoorbellFault { errno: ENOSYS })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DoorbellFault, TRANSIENT_FAULT_RETRIES, WaitOutcome, platform, respond_to, wait_retrying,
    };
    use core::time::Duration;
    use std::cell::Cell;
    use std::time::Instant;

    fn fault(errno: i32) -> DoorbellFault {
        DoorbellFault { errno }
    }

    /// A wait whose address is fine but whose host is momentarily short of memory settles on
    /// the answer it was waiting for, rather than reporting a permanent failure.
    #[test]
    fn a_transient_fault_is_absorbed_and_the_wait_settles() {
        let attempts = Cell::new(0_u32);
        let outcome = wait_retrying(Some(Duration::from_secs(5)), true, |_| {
            attempts.set(attempts.get() + 1);
            if attempts.get() <= 3 {
                return Err(fault(platform::EFAULT));
            }
            Ok(WaitOutcome::Woken)
        });
        assert_eq!(outcome, Ok(WaitOutcome::Woken));
        assert_eq!(attempts.get(), 4);
    }

    /// The absorption is bounded: an address that faults forever is a typed fault, not a
    /// loop, and it carries the platform's own error number.
    #[test]
    fn a_transient_fault_past_its_budget_is_surfaced() {
        let attempts = Cell::new(0_u32);
        let outcome = wait_retrying(Some(Duration::from_secs(5)), true, |_| {
            attempts.set(attempts.get() + 1);
            Err(fault(platform::EFAULT))
        });
        assert_eq!(outcome, Err(fault(platform::EFAULT)));
        assert_eq!(attempts.get(), TRANSIENT_FAULT_RETRIES + 1);
    }

    /// The probe's semantics: where a fault is not read as transient, the first one decides,
    /// which is what keeps [`super::waiting_is_supported`] answering the placement question
    /// pessimistically rather than retrying past it.
    #[test]
    fn a_fault_that_is_not_transient_is_surfaced_on_the_first_attempt() {
        let attempts = Cell::new(0_u32);
        let outcome = wait_retrying(Some(Duration::from_secs(5)), false, |_| {
            attempts.set(attempts.get() + 1);
            Err(fault(platform::EFAULT))
        });
        assert_eq!(outcome, Err(fault(platform::EFAULT)));
        assert_eq!(attempts.get(), 1);
    }

    /// An interruption is not a fault and spends none of the transient budget: a wait
    /// interrupted more often than the budget allows still settles.
    #[test]
    fn interruptions_do_not_spend_the_transient_budget() {
        let attempts = Cell::new(0_u32);
        let interruptions = TRANSIENT_FAULT_RETRIES * 4;
        let outcome = wait_retrying(Some(Duration::from_secs(5)), true, |_| {
            attempts.set(attempts.get() + 1);
            if attempts.get() <= interruptions {
                return Err(fault(platform::EINTR));
            }
            Ok(WaitOutcome::Woken)
        });
        assert_eq!(outcome, Ok(WaitOutcome::Woken));
        assert_eq!(attempts.get(), interruptions + 1);
    }

    /// The caller's deadline outranks the retry policy: a wait that faults its way through a
    /// deadline reports a timeout rather than spending its whole budget past one.
    #[test]
    fn the_deadline_outranks_the_retry_budget() {
        let started = Instant::now();
        let outcome = wait_retrying(Some(Duration::from_millis(1)), true, |remaining| {
            std::thread::sleep(remaining.expect("a bounded wait always hands over a remainder"));
            Err(fault(platform::EFAULT))
        });
        assert_eq!(outcome, Ok(WaitOutcome::TimedOut));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the retry budget outlived the deadline: took {:?}",
            started.elapsed()
        );
    }

    /// Every response the policy can give, named directly, so the table itself is pinned and
    /// not only its effect through the loop.
    #[test]
    fn the_policy_names_a_response_for_every_class() {
        use super::FaultResponse::{RetryAfterBackoff, RetryNow, Surface};
        assert_eq!(respond_to(platform::EINTR, false, 0), RetryNow);
        assert_eq!(
            respond_to(platform::ENOMEM, false, TRANSIENT_FAULT_RETRIES),
            RetryNow
        );
        assert_eq!(respond_to(platform::EFAULT, true, 0), RetryAfterBackoff);
        assert_eq!(
            respond_to(platform::EFAULT, true, TRANSIENT_FAULT_RETRIES),
            Surface
        );
        assert_eq!(respond_to(platform::EFAULT, false, 0), Surface);
        assert_eq!(respond_to(1, true, 0), Surface);
    }
}
