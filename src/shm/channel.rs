//! The authenticated local attachment channel: descriptor transfer and peer credentials over
//! a connected Unix domain socket.
//!
//! `docs/notes/shared-memory-model.md` §4.2 states the requirement this implements. A
//! consumer is meant to reach a segment by **descriptor transfer** rather than by opening a
//! path it could race or guess: possession of the descriptor is the authorization, the daemon
//! opens it read-only before handing it over, and the segment's own file permissions stay the
//! primary gate. Peer credentials are the check on top — the daemon learns the connected
//! peer's effective user before it transfers anything — and never a replacement for those
//! permissions.
//!
//! The primitives are declared by hand rather than pulled from a binding crate, exactly as
//! [`super::doorbell`]'s wait and wake are: the daemon takes no dependency it does not need.
//! Ancillary data is where the two platforms differ most, so the whole platform delta is one
//! `platform` module: the control-message alignment (`__DARWIN_ALIGN32`'s four bytes against
//! Linux's word), the widths `msghdr` and `cmsghdr` declare their lengths at, and how a peer's
//! effective user is asked for at all — Linux answers `SO_PEERCRED`, Darwin `getpeereid(2)`.
//!
//! Nothing here interprets the payload it carries. The caller passes the bytes it wants sent —
//! for `pmwsd` that is the control protocol's own answer line — and the descriptors ride the
//! same `sendmsg`, so a reader can never see the line without the descriptors or the
//! descriptors without the line.
#![allow(unsafe_code)]

use core::ffi::{c_int, c_void};
use platform::{CmsgLen, ControlLen, IovLen};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// The most descriptors one message of this channel carries: a segment, and the sibling
/// doorbell page a segment declaring [`super::FEATURE_DOORBELL_PAGE`] needs beside it.
///
/// A bound rather than a convention: the control buffer is sized from it, so a peer that
/// attaches more descriptors than this has them truncated by the kernel and the receiver
/// answers [`ChannelError::ControlTruncated`] instead of silently adopting an unbounded
/// number of open files.
pub const MAX_TRANSFERRED_DESCRIPTORS: usize = 2;

/// Why a descriptor-carrying receive could not be trusted.
///
/// [`Self::ControlTruncated`] is the one condition a raw `recvmsg` reports only in a returned
/// flag: the kernel had more ancillary data than the control buffer could hold and dropped
/// the remainder. Treating that as a short success is how a receiver ends up using half a
/// transfer, so it is a typed error here, and whatever descriptors did arrive are closed
/// before it is returned rather than leaked.
#[derive(Debug)]
pub enum ChannelError {
    Io(io::Error),
    ControlTruncated,
}

impl core::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "attachment channel i/o failed: {error}"),
            Self::ControlTruncated => {
                f.write_str("attachment channel message carried more ancillary data than fits")
            }
        }
    }
}
impl std::error::Error for ChannelError {}

impl From<io::Error> for ChannelError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ChannelError> for io::Error {
    fn from(error: ChannelError) -> Self {
        match error {
            ChannelError::Io(error) => error,
            ChannelError::ControlTruncated => io::Error::new(io::ErrorKind::InvalidData, error),
        }
    }
}

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
/// the time the connection was made, which is the identity a descriptor transfer is
/// authorized against; it is deliberately not a process identifier, because a pid may be
/// reused and is never identity on its own (`docs/notes/shared-memory-model.md` §4.2).
///
/// Fails with the platform's own error for a socket that is not connected, is not
/// `AF_UNIX`, or on which the platform declines to report credentials.
pub fn peer_euid(socket: BorrowedFd<'_>) -> io::Result<u32> {
    platform::peer_euid(socket)
}

/// Sends `payload` and `descriptors` as one message on the connected socket `socket`.
///
/// One `sendmsg` carries both, so the two can never be separated in the stream: a receiver
/// that reads the payload has the descriptors that belong to it, and there is no framing
/// question about which answer a descriptor belongs to. Each descriptor is duplicated into
/// the receiving process by the kernel; the caller keeps its own and closes it as usual.
///
/// `payload` must be non-empty — a stream socket does not reliably carry ancillary data with
/// no data of its own — and at most [`MAX_TRANSFERRED_DESCRIPTORS`] descriptors may ride one
/// message; both are refused with [`io::ErrorKind::InvalidInput`]. An empty `descriptors`
/// sends no control message at all rather than an empty one, which some kernels refuse.
///
/// Answers how many payload bytes the kernel accepted, which on a stream socket may be fewer
/// than were offered; the descriptors are attached to the bytes that were accepted, so a
/// caller sending a longer payload writes the remainder ordinarily. Fails with the platform's
/// own error, `EINTR` excepted: an interrupted send is retried, because no byte and no
/// descriptor has been transferred when it happens.
pub fn send_with_fds(
    socket: BorrowedFd<'_>,
    payload: &[u8],
    descriptors: &[BorrowedFd<'_>],
) -> io::Result<usize> {
    if payload.is_empty() || descriptors.len() > MAX_TRANSFERRED_DESCRIPTORS {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let mut control = ControlBuffer::new();
    let raw: Vec<RawFd> = descriptors.iter().map(AsRawFd::as_raw_fd).collect();
    let control_bytes = control.write_rights(raw.as_slice());
    let mut iov = IoVec {
        base: payload.as_ptr().cast_mut().cast::<c_void>(),
        len: payload.len(),
    };
    let message = MsgHdr {
        name: core::ptr::null_mut(),
        name_len: 0,
        iov: &raw mut iov,
        iov_len: 1,
        control: if control_bytes == 0 {
            core::ptr::null_mut()
        } else {
            control.as_mut_ptr().cast::<c_void>()
        },
        control_len: control_len(control_bytes),
        flags: 0,
    };
    loop {
        // SAFETY: `message` names one `iovec` describing `payload`'s own bytes and, when
        // descriptors were given, a control buffer this call owns and has just filled with a
        // single well-formed `SCM_RIGHTS` header; every length field describes exactly those
        // allocations. The kernel reads through them and writes nothing back.
        let sent = unsafe {
            platform::sendmsg(socket.as_raw_fd(), &raw const message, platform::SEND_FLAGS)
        };
        if let Ok(sent) = usize::try_from(sent) {
            return Ok(sent);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Receives one message from the connected socket `socket` into `buffer`, adopting up to
/// `max_descriptors` descriptors that rode with it.
///
/// Answers how many payload bytes arrived — `0` for an orderly peer shutdown — and the
/// descriptors, already owned: each is closed when the returned [`OwnedFd`] is dropped, so a
/// caller that abandons the message leaks nothing.
///
/// A message carrying more ancillary data than `max_descriptors` fits is
/// [`ChannelError::ControlTruncated`], and the descriptors that did arrive are closed rather
/// than returned: a truncated transfer is never half-usable. `max_descriptors` above
/// [`MAX_TRANSFERRED_DESCRIPTORS`], or an empty `buffer`, is
/// [`io::ErrorKind::InvalidInput`]. An interrupted receive is retried, because nothing has
/// been consumed when that happens.
pub fn recv_with_fds(
    socket: BorrowedFd<'_>,
    buffer: &mut [u8],
    max_descriptors: usize,
) -> Result<(usize, Vec<OwnedFd>), ChannelError> {
    if buffer.is_empty() || max_descriptors > MAX_TRANSFERRED_DESCRIPTORS {
        return Err(ChannelError::Io(io::Error::from(
            io::ErrorKind::InvalidInput,
        )));
    }
    let mut control = ControlBuffer::new();
    let capacity = cmsg_space(max_descriptors * size_of::<RawFd>());
    let mut iov = IoVec {
        base: buffer.as_mut_ptr().cast::<c_void>(),
        len: buffer.len(),
    };
    let received = loop {
        let mut message = MsgHdr {
            name: core::ptr::null_mut(),
            name_len: 0,
            iov: &raw mut iov,
            iov_len: 1,
            control: if max_descriptors == 0 {
                core::ptr::null_mut()
            } else {
                control.as_mut_ptr().cast::<c_void>()
            },
            control_len: control_len(if max_descriptors == 0 { 0 } else { capacity }),
            flags: 0,
        };
        // SAFETY: `message` names one `iovec` over `buffer` and a control buffer this call
        // owns, both described by their true lengths; the kernel writes only inside them and
        // into `message`'s own `control_len` and `flags` fields.
        let read = unsafe {
            platform::recvmsg(socket.as_raw_fd(), &raw mut message, platform::RECV_FLAGS)
        };
        if let Ok(read) = usize::try_from(read) {
            let truncated = message.flags & platform::MSG_CTRUNC != 0;
            let written = platform_len(message.control_len);
            break (read, written.min(capacity), truncated);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(ChannelError::Io(error));
        }
    };
    let (read, control_bytes, truncated) = received;
    let descriptors = control.adopt_rights(control_bytes);
    if truncated || descriptors.len() > max_descriptors {
        return Err(ChannelError::ControlTruncated);
    }
    Ok((read, descriptors))
}

/// The ancillary-data buffer one message of this channel is built in.
///
/// Over-aligned to eight bytes so a `cmsghdr` may be written at its start on either platform
/// — Linux's carries a `size_t` length and needs word alignment, Darwin's a `socklen_t` and
/// needs four — and sized for [`MAX_TRANSFERRED_DESCRIPTORS`] descriptors, which is the whole
/// reason that constant is a bound and not a convention.
#[repr(C, align(8))]
struct ControlBuffer([u8; CONTROL_BUFFER_BYTES]);

const CONTROL_BUFFER_BYTES: usize = cmsg_space(MAX_TRANSFERRED_DESCRIPTORS * size_of::<RawFd>());

impl ControlBuffer {
    fn new() -> Self {
        Self([0; CONTROL_BUFFER_BYTES])
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    /// Writes one `SCM_RIGHTS` control message carrying `descriptors` and answers how many
    /// bytes of this buffer it occupies — `0` for no descriptors at all, which is the
    /// caller's signal to send no control message rather than an empty one.
    fn write_rights(&mut self, descriptors: &[RawFd]) -> usize {
        if descriptors.is_empty() {
            return 0;
        }
        let payload = core::mem::size_of_val(descriptors);
        let header = CmsgHdr {
            len: CmsgLen::try_from(cmsg_len(payload)).unwrap_or(CmsgLen::MAX),
            level: platform::SOL_SOCKET,
            kind: platform::SCM_RIGHTS,
        };
        // SAFETY: this buffer is at least `cmsg_space(payload)` bytes and is aligned to eight,
        // so a `CmsgHdr` write at offset zero is in bounds and aligned, and the descriptor
        // bytes at `CMSG_HEADER_BYTES` are in bounds by the same arithmetic. The descriptors
        // are copied as bytes, so no alignment is required of the destination.
        unsafe {
            self.0.as_mut_ptr().cast::<CmsgHdr>().write(header);
            core::ptr::copy_nonoverlapping(
                descriptors.as_ptr().cast::<u8>(),
                self.0.as_mut_ptr().add(CMSG_HEADER_BYTES),
                payload,
            );
        }
        cmsg_space(payload)
    }

    /// Adopts every descriptor the kernel placed in the first `filled` bytes of this buffer.
    ///
    /// Walks the control messages the way the platform's own `CMSG_NXTHDR` does, refusing a
    /// header whose declared length is shorter than a header or longer than what the kernel
    /// says it wrote: a length under the header size would underflow the payload arithmetic,
    /// which is the classic defect of a hand-rolled `SCM_RIGHTS` parse.
    fn adopt_rights(&self, filled: usize) -> Vec<OwnedFd> {
        let mut adopted = Vec::new();
        let mut offset = 0;
        while filled.saturating_sub(offset) >= CMSG_HEADER_BYTES {
            // SAFETY: `offset + CMSG_HEADER_BYTES <= filled <= CONTROL_BUFFER_BYTES`, and the
            // buffer is aligned to eight while every step below advances by a `cmsg_align`
            // multiple, so the read is in bounds and aligned for `CmsgHdr`.
            let header = unsafe { self.0.as_ptr().add(offset).cast::<CmsgHdr>().read() };
            let declared = platform_len(header.len);
            if declared < cmsg_len(0) || declared > filled - offset {
                break;
            }
            if header.level == platform::SOL_SOCKET && header.kind == platform::SCM_RIGHTS {
                let payload = declared - CMSG_HEADER_BYTES;
                for index in 0..payload / size_of::<RawFd>() {
                    let mut raw = [0_u8; size_of::<RawFd>()];
                    let at = offset + CMSG_HEADER_BYTES + index * size_of::<RawFd>();
                    raw.copy_from_slice(&self.0[at..at + size_of::<RawFd>()]);
                    let descriptor = RawFd::from_ne_bytes(raw);
                    // SAFETY: the kernel installed `descriptor` in this process as part of
                    // this receive, and it is read exactly once here, so this is the only
                    // owner it will ever have.
                    adopted.push(unsafe { OwnedFd::from_raw_fd(descriptor) });
                }
            }
            offset += cmsg_space(declared - CMSG_HEADER_BYTES);
        }
        platform::keep_descriptors_out_of_children(adopted.as_slice());
        adopted
    }
}

#[repr(C)]
struct IoVec {
    base: *mut c_void,
    len: usize,
}

/// `struct msghdr`, whose field order is identical on both platforms and whose two length
/// fields are not: Linux declares them `size_t`, Darwin an `int` and a `socklen_t`.
#[repr(C)]
struct MsgHdr {
    name: *mut c_void,
    name_len: u32,
    iov: *mut IoVec,
    iov_len: IovLen,
    control: *mut c_void,
    control_len: ControlLen,
    flags: c_int,
}

/// `struct cmsghdr`, whose length field is a `size_t` on Linux and a `socklen_t` on Darwin.
#[repr(C)]
struct CmsgHdr {
    len: CmsgLen,
    level: c_int,
    kind: c_int,
}

/// Widens a platform length word — `usize` on Linux, `u32` on Darwin — to `usize`.
///
/// A value the platform type cannot represent as `usize` collapses to zero, which every
/// caller treats as an empty control region rather than an error.
fn platform_len(value: impl TryInto<usize>) -> usize {
    value.try_into().unwrap_or(0)
}

fn control_len(bytes: usize) -> ControlLen {
    ControlLen::try_from(bytes).unwrap_or(ControlLen::MAX)
}

/// `CMSG_ALIGN`: the platform's own rounding for a control message's parts — a word on Linux,
/// four bytes on Darwin, where the macro is spelled `__DARWIN_ALIGN32`.
const fn cmsg_align(bytes: usize) -> usize {
    let alignment = platform::CMSG_ALIGNMENT;
    bytes.div_ceil(alignment) * alignment
}

/// Where a control message's payload starts, which is `CMSG_DATA`'s own offset: the aligned
/// size of the header. Sixteen bytes on Linux, twelve on Darwin.
const CMSG_HEADER_BYTES: usize = cmsg_align(size_of::<CmsgHdr>());

/// `CMSG_LEN`: what a control message declares as its length for a payload of `bytes`.
const fn cmsg_len(bytes: usize) -> usize {
    CMSG_HEADER_BYTES + bytes
}

/// `CMSG_SPACE`: how many bytes of a control buffer a payload of `bytes` occupies, padding
/// included.
const fn cmsg_space(bytes: usize) -> usize {
    CMSG_HEADER_BYTES + cmsg_align(bytes)
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{CmsgHdr, MsgHdr};
    use core::ffi::{c_int, c_void};
    use std::io;
    use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

    pub(super) type IovLen = usize;
    pub(super) type ControlLen = usize;
    pub(super) type CmsgLen = usize;

    pub(super) const CMSG_ALIGNMENT: usize = size_of::<usize>();
    pub(super) const SOL_SOCKET: c_int = 1;
    pub(super) const SCM_RIGHTS: c_int = 1;
    pub(super) const MSG_CTRUNC: c_int = 0x8;
    /// `MSG_NOSIGNAL`: a peer that closed the socket is an `EPIPE` answer rather than a
    /// signal, so a daemon serving an attachment is never killed by a consumer that left.
    pub(super) const SEND_FLAGS: c_int = 0x4000;
    /// `MSG_CMSG_CLOEXEC`: a descriptor arrives already close-on-exec, so a receiver that
    /// forks and execs between the receive and its own `fcntl` cannot leak the segment into
    /// an unrelated program.
    pub(super) const RECV_FLAGS: c_int = 0x4000_0000;

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
        pub(super) fn sendmsg(socket: c_int, message: *const MsgHdr, flags: c_int) -> isize;
        pub(super) fn recvmsg(socket: c_int, message: *mut MsgHdr, flags: c_int) -> isize;
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

    /// Nothing to do: [`RECV_FLAGS`] already asked the kernel for close-on-exec descriptors.
    pub(super) fn keep_descriptors_out_of_children(_descriptors: &[OwnedFd]) {}

    const _: () = assert!(size_of::<CmsgHdr>() == 16);
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{CmsgHdr, MsgHdr};
    use core::ffi::c_int;
    use std::io;
    use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

    pub(super) type IovLen = c_int;
    pub(super) type ControlLen = u32;
    pub(super) type CmsgLen = u32;

    /// `__DARWIN_ALIGN32`: Darwin aligns a control message's parts to four bytes, not to a
    /// word, which is why `CMSG_DATA` sits twelve bytes into a header Linux puts sixteen
    /// bytes into.
    pub(super) const CMSG_ALIGNMENT: usize = size_of::<u32>();
    pub(super) const SOL_SOCKET: c_int = 0xffff;
    pub(super) const SCM_RIGHTS: c_int = 1;
    pub(super) const MSG_CTRUNC: c_int = 0x20;
    /// Darwin has no `MSG_NOSIGNAL`; a Rust process ignores `SIGPIPE` from startup, and so do
    /// the Python and Node runtimes this channel's client half is loaded into, so a peer that
    /// left is an `EPIPE` answer there too.
    pub(super) const SEND_FLAGS: c_int = 0;
    /// Darwin has no `MSG_CMSG_CLOEXEC`; [`keep_descriptors_out_of_children`] sets the flag
    /// on each descriptor immediately after the receive instead.
    pub(super) const RECV_FLAGS: c_int = 0;

    const F_SETFD: c_int = 2;
    const FD_CLOEXEC: c_int = 1;

    unsafe extern "C" {
        pub(super) fn sendmsg(socket: c_int, message: *const MsgHdr, flags: c_int) -> isize;
        pub(super) fn recvmsg(socket: c_int, message: *mut MsgHdr, flags: c_int) -> isize;
        pub(super) fn geteuid() -> u32;
        fn getpeereid(socket: c_int, euid: *mut u32, egid: *mut u32) -> c_int;
        fn fcntl(descriptor: c_int, command: c_int, ...) -> c_int;
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

    /// Marks each received descriptor close-on-exec, which Linux's `MSG_CMSG_CLOEXEC` does in
    /// the receive itself.
    ///
    /// Best effort by construction: the window this closes is between the receive and this
    /// call, and a failure here leaves a descriptor inheritable rather than unusable, so it
    /// is not worth failing an otherwise complete transfer over.
    pub(super) fn keep_descriptors_out_of_children(descriptors: &[OwnedFd]) {
        for descriptor in descriptors {
            // SAFETY: `descriptor` is a live descriptor this process owns; `F_SETFD` reads one
            // `int` argument and touches no memory.
            let _set = unsafe { fcntl(descriptor.as_raw_fd(), F_SETFD, FD_CLOEXEC) };
        }
    }

    const _: () = assert!(size_of::<CmsgHdr>() == 12);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixStream;

    /// A file holding `contents`, opened fresh read-only so its transferred descriptor starts
    /// at offset zero exactly as the daemon's does.
    fn readable_file(tag: &str, contents: &str) -> (std::path::PathBuf, std::fs::File) {
        let path = std::env::temp_dir().join(format!(
            "pmws-channel-{tag}-{}-{:?}.txt",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, contents).expect("the fixture is written");
        let file = std::fs::File::open(&path).expect("the fixture opens");
        (path, file)
    }

    fn read_all(descriptor: OwnedFd) -> String {
        let mut text = String::new();
        let _read = std::fs::File::from(descriptor)
            .read_to_string(&mut text)
            .expect("the transferred descriptor reads");
        text
    }

    #[test]
    fn a_message_with_no_descriptors_round_trips_its_payload() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let sent = send_with_fds(sender.as_fd(), b"attach\n", &[]).expect("the send succeeds");
        assert_eq!(sent, 7);
        let mut buffer = [0_u8; 32];
        let (read, descriptors) =
            recv_with_fds(receiver.as_fd(), &mut buffer, MAX_TRANSFERRED_DESCRIPTORS)
                .expect("the receive succeeds");
        assert_eq!(&buffer[..read], b"attach\n");
        assert!(
            descriptors.is_empty(),
            "no control message was sent, so none arrives"
        );
    }

    #[test]
    fn one_descriptor_rides_the_same_message_as_its_payload() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let (path, file) = readable_file("one", "segment");
        let sent =
            send_with_fds(sender.as_fd(), b"{}\n", &[file.as_fd()]).expect("the send succeeds");
        assert_eq!(sent, 3);
        let mut buffer = [0_u8; 32];
        let (read, mut descriptors) =
            recv_with_fds(receiver.as_fd(), &mut buffer, MAX_TRANSFERRED_DESCRIPTORS)
                .expect("the receive succeeds");
        assert_eq!(&buffer[..read], b"{}\n");
        assert_eq!(descriptors.len(), 1, "one descriptor was attached");
        assert_eq!(read_all(descriptors.remove(0)), "segment");
        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn two_descriptors_arrive_in_the_order_they_were_sent() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let (segment_path, segment) = readable_file("two-a", "segment");
        let (page_path, page) = readable_file("two-b", "doorbell");
        let _sent = send_with_fds(sender.as_fd(), b"{}\n", &[segment.as_fd(), page.as_fd()])
            .expect("the send succeeds");
        let mut buffer = [0_u8; 32];
        let (_read, descriptors) =
            recv_with_fds(receiver.as_fd(), &mut buffer, MAX_TRANSFERRED_DESCRIPTORS)
                .expect("the receive succeeds");
        assert_eq!(descriptors.len(), 2, "both descriptors were attached");
        let contents: Vec<String> = descriptors.into_iter().map(read_all).collect();
        assert_eq!(
            contents,
            vec!["segment".to_owned(), "doorbell".to_owned()],
            "the receiver sees the descriptors in send order"
        );
        let _removed = std::fs::remove_file(segment_path);
        let _removed = std::fs::remove_file(page_path);
    }

    #[test]
    fn a_receiver_expecting_fewer_descriptors_than_arrive_reports_truncation() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let (segment_path, segment) = readable_file("truncated-a", "segment");
        let (page_path, page) = readable_file("truncated-b", "doorbell");
        let _sent = send_with_fds(sender.as_fd(), b"{}\n", &[segment.as_fd(), page.as_fd()])
            .expect("the send succeeds");
        let mut buffer = [0_u8; 32];
        let outcome = recv_with_fds(receiver.as_fd(), &mut buffer, 1);
        assert!(
            matches!(outcome, Err(ChannelError::ControlTruncated)),
            "a control buffer too small for the transfer is a typed error, not a short success: {outcome:?}"
        );
        let _removed = std::fs::remove_file(segment_path);
        let _removed = std::fs::remove_file(page_path);
    }

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

    #[test]
    fn an_empty_payload_and_an_oversized_transfer_are_refused() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let (path, file) = readable_file("refused", "segment");
        let empty = send_with_fds(sender.as_fd(), b"", &[file.as_fd()]);
        assert_eq!(
            empty.map_err(|error| error.kind()).unwrap_err(),
            io::ErrorKind::InvalidInput,
            "ancillary data with no payload is refused rather than sent"
        );
        let mut buffer = [0_u8; 8];
        let over = recv_with_fds(
            receiver.as_fd(),
            &mut buffer,
            MAX_TRANSFERRED_DESCRIPTORS + 1,
        );
        assert!(
            matches!(over, Err(ChannelError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput),
            "a receiver may not ask for more descriptors than this channel bounds"
        );
        let _removed = std::fs::remove_file(path);
    }

    /// The payload and the descriptors are one message, so a receiver that reads the line has
    /// the descriptors that belong to it — never the line first and the transfer later.
    #[test]
    fn a_second_receive_finds_nothing_left_of_the_transfer() {
        let (sender, receiver) = UnixStream::pair().expect("a socket pair");
        let (path, file) = readable_file("single", "segment");
        let _sent = send_with_fds(sender.as_fd(), b"line\n", &[file.as_fd()]);
        let mut buffer = [0_u8; 32];
        let (read, descriptors) =
            recv_with_fds(receiver.as_fd(), &mut buffer, MAX_TRANSFERRED_DESCRIPTORS)
                .expect("the receive succeeds");
        assert_eq!(read, 5);
        assert_eq!(descriptors.len(), 1);
        drop(sender);
        let (closed, none) =
            recv_with_fds(receiver.as_fd(), &mut buffer, MAX_TRANSFERRED_DESCRIPTORS)
                .expect("the peer's shutdown is an orderly end");
        assert_eq!(closed, 0, "one message carried the whole transfer");
        assert!(none.is_empty());
        let _removed = std::fs::remove_file(path);
    }

    #[test]
    fn the_platform_control_arithmetic_matches_the_headers() {
        assert_eq!(CMSG_HEADER_BYTES, cmsg_align(size_of::<CmsgHdr>()));
        assert_eq!(cmsg_len(0), CMSG_HEADER_BYTES);
        assert!(cmsg_space(4) >= cmsg_len(4));
        assert!(
            CONTROL_BUFFER_BYTES >= cmsg_space(MAX_TRANSFERRED_DESCRIPTORS * size_of::<RawFd>()),
            "the control buffer holds the descriptors this channel bounds itself to"
        );
    }
}
