//! Effective-user credentials for a connected local peer.
//!
//! The control rail accepts commands over a Unix domain socket and answers only a peer
//! running as the same effective user this process does. The two primitives are declared by
//! hand rather than pulled from a binding crate: the daemon takes no dependency it does not
//! need, and how a peer's effective user is asked for is the whole platform delta — Linux
//! answers `SO_PEERCRED`, Darwin `getpeereid(2)`.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::BorrowedFd;

/// The effective user this process runs as, as the operating system reports it.
///
/// Infallible by definition — `geteuid(2)` cannot fail — which is what lets a caller compare
/// it against [`peer_euid`] without a second error path.
pub fn own_euid() -> u32 {
    // SAFETY: `geteuid` takes no argument, touches no memory this process owns, and is
    // documented never to fail.
    unsafe { platform::geteuid() }
}

/// The effective user of the peer connected to `socket`.
///
/// `socket` must be a connected `AF_UNIX` stream. The answer is the peer's effective user at
/// the time the connection was made, which is the identity a local command is authorized
/// against; it is deliberately not a process identifier, because a pid may be reused and is
/// never identity on its own.
///
/// Fails with the platform's own error for a socket that is not connected, is not
/// `AF_UNIX`, or on which the platform declines to report credentials.
pub fn peer_euid(socket: BorrowedFd<'_>) -> io::Result<u32> {
    platform::peer_euid(socket)
}

#[cfg(target_os = "linux")]
mod platform {
    use core::ffi::{c_int, c_void};
    use std::io;
    use std::os::fd::{AsRawFd, BorrowedFd};

    const SOL_SOCKET: c_int = 1;
    const SO_PEERCRED: c_int = 17;

    /// `struct ucred`: the connected peer's process, user and group as the kernel recorded
    /// them at connect time.
    #[repr(C)]
    struct Ucred {
        pid: u32,
        uid: u32,
        gid: u32,
    }

    unsafe extern "C" {
        pub(super) fn geteuid() -> u32;
        fn getsockopt(
            socket: c_int,
            level: c_int,
            name: c_int,
            value: *mut c_void,
            length: *mut u32,
        ) -> c_int;
    }

    pub(super) fn peer_euid(socket: BorrowedFd<'_>) -> io::Result<u32> {
        let mut credentials = Ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = size_of::<Ucred>() as u32;
        // SAFETY: `credentials` is a live, correctly sized `struct ucred` this call owns, and
        // `length` names its size; the kernel writes at most that many bytes into it and the
        // length it actually wrote back into `length`.
        let outcome = unsafe {
            getsockopt(
                socket.as_raw_fd(),
                SOL_SOCKET,
                SO_PEERCRED,
                (&raw mut credentials).cast::<c_void>(),
                &raw mut length,
            )
        };
        if outcome != 0 {
            return Err(io::Error::last_os_error());
        }
        if length as usize != size_of::<Ucred>() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(credentials.uid)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use core::ffi::c_int;
    use std::io;
    use std::os::fd::{AsRawFd, BorrowedFd};

    unsafe extern "C" {
        pub(super) fn geteuid() -> u32;
        fn getpeereid(socket: c_int, euid: *mut u32, egid: *mut u32) -> c_int;
    }

    pub(super) fn peer_euid(socket: BorrowedFd<'_>) -> io::Result<u32> {
        let mut euid = 0_u32;
        let mut egid = 0_u32;
        // SAFETY: both out-parameters are live locals this call owns, and `getpeereid` writes
        // one `uid_t` and one `gid_t` into them and nothing else.
        let outcome = unsafe { getpeereid(socket.as_raw_fd(), &raw mut euid, &raw mut egid) };
        if outcome != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(euid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn a_connected_peer_reports_this_process_own_effective_user() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let euid = own_euid();
        assert_eq!(
            peer_euid(sender.as_fd()).expect("the peer's user is readable"),
            euid,
            "both ends of a pair this process made run as this process"
        );
        assert_eq!(
            peer_euid(receiver.as_fd()).expect("the peer's user is readable"),
            euid
        );
    }
}
