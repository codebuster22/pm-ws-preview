//! The typed cell layer: the only way any byte of a publication region is reached.
//!
//! `docs/notes/shared-memory-model.md` §1 fixes four cell classes and one access rule for
//! each. Those rules are carried here by types rather than by prose, so an illegal access
//! cannot be written down: a [`SyncCell`] has no plain load, a [`RelaxedWord64`] has no
//! acquire or release, and write-once bytes are reachable only through a
//! [`PublishedWitness`] that an acquire load of the governing gate produced. No item here
//! hands out a reference to a mutable cell.
#![allow(unsafe_code)]

use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// The byte boundary every publication region, and every cache-line-separated record inside
/// one, is laid out to.
///
/// This is the alignment this *build* assumes, not a value read from the running host: 128
/// bytes, matching `hw.cachelinesize` on the `aarch64-apple-darwin` execution profile and
/// satisfying a 64-byte host at the cost of padding. The segment header carries it so a
/// reader built under a different assumption fails validation instead of reading a
/// differently padded segment.
pub const REGION_ALIGNMENT: usize = 128;

/// The largest region this module will allocate, in bytes.
pub const MAX_REGION_BYTES: usize = 1 << 30;

/// The value a write-once gate holds between being claimed and being published.
///
/// A record whose gate reads this has a formatter inside it, or had one that died there.
/// It is deliberately a value no gate of this ABI ever publishes, so it can never validate:
/// [`Record::published`] treats it exactly like zero, and a reader sees an unpublished
/// record rather than a half-written one.
pub(super) const GATE_CLAIMED: u64 = u64::MAX;

#[repr(C, align(128))]
struct AlignedBlock(UnsafeCell<[u8; REGION_ALIGNMENT]>);

const _: () = assert!(REGION_ALIGNMENT == core::mem::align_of::<AlignedBlock>());
const _: () = assert!(REGION_ALIGNMENT == core::mem::size_of::<AlignedBlock>());

/// Why a publication region could not be created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionError {
    SizeZero,
    SizeNotBlockAligned,
    SizeTooLarge,
    /// The kernel placed the mapping at an address the layout cannot use. Every platform
    /// this runs on maps on a page boundary of at least 4096 bytes, which already satisfies
    /// [`REGION_ALIGNMENT`], so this is a fail-closed guard rather than an expected outcome.
    MappingMisaligned,
    Io(std::io::ErrorKind),
}
impl core::fmt::Display for RegionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(kind) => write!(f, "publication region i/o failed: {kind}"),
            other => write!(f, "invalid publication region: {other:?}"),
        }
    }
}
impl From<std::io::Error> for RegionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}
impl std::error::Error for RegionError {}

/// The owning handle for a region's memory.
///
/// Never read after construction: its whole job is to keep the allocation or the mapping
/// alive for exactly as long as the [`SegmentRegion`] that captured `base` from it, and to
/// unmap or free it on drop.
#[expect(
    dead_code,
    reason = "ownership handle: keeps the backing alive for `base`"
)]
enum RegionBacking {
    Heap(Box<[AlignedBlock]>),
    Writable(MmapMut),
    ReadOnly(Mmap),
}

/// Backing memory for one publication segment: an aligned heap allocation, a read-write
/// file mapping, or a read-only file mapping.
///
/// This is the single seam every other type is written against. A writer and a reader are
/// handed the region and reach it only through [`RegionCells`], so neither knows which
/// backing it has, and the in-process and cross-process cases run the same code.
///
/// A live segment is never resized and never remapped: [`Self::create_file`] sets the
/// file's length once, before the mapping exists, and no method here exposes a resize.
pub struct SegmentRegion {
    #[expect(
        dead_code,
        reason = "ownership handle: keeps the backing alive for `base`"
    )]
    backing: RegionBacking,
    base: *mut u8,
    size: usize,
    writable: bool,
    attachment: u64,
    path: Option<std::path::PathBuf>,
    creation: Option<CreationObject>,
}

/// The object a region was created as, held for the region's life.
///
/// A pathname is a mutable name: anything running as this user can move a file out from
/// under one and put another in its place, so every later question about "the segment" that
/// is answered by resolving the name again is answered about whatever the name resolves to
/// *now*. This is the answer that cannot drift — the open file description the exclusive
/// create returned, and the `fstat` of that description rather than an `lstat` of the name.
/// It is what the doorbell probe maps and what the daemon compares every later descriptor
/// against.
struct CreationObject {
    file: File,
    identity: (u64, u64),
}

/// Hands out one identity per region this process constructs.
static NEXT_ATTACHMENT: AtomicU64 = AtomicU64::new(1);

// SAFETY: every byte of the backing is reachable only through `RegionCells`, which yields
// `SyncCell`, `RelaxedWord32`, `RelaxedWord64` and `PublishedWitness` at fixed, naturally
// aligned, width-consistent offsets. No byte is ever accessed at two widths, and the only
// non-atomic accesses are to `write_once_published` bytes, which `publish_write_once`
// writes strictly before the release store of their gate and which a reader can reach only
// after an acquire load of that gate — so no non-atomic access ever races. Those writes are
// serialized by the atomic claim in `publish_write_once`: a record's gate is
// compare-exchanged from zero to `GATE_CLAIMED` before any payload byte is touched, so at
// most one thread ever writes a given record's payload, and every other thread observes the
// claim and writes nothing. The only bulk
// non-atomic writes are the zero initializations inside `zeroed` and `create_file`, which
// happen before the region is shared at all. `base` is captured once at construction and
// stays valid because the backing it names is owned by this value, is never resized and is
// never remapped, and every cell borrows from `&self` so no cell can outlive the mapping.
// Sending the region between threads therefore moves no thread-affine state, and sharing it
// lets two threads reach only atomics and settled bytes. Data-race freedom is what this
// argument establishes; *consistency* of a multi-word record is a separate property, and it
// comes from `SeqCell`'s fenced even-odd construction rather than from atomicity alone.
//
// Two hazards are outside this argument and are accepted, not solved: a store through a
// read-only mapping is a SIGSEGV rather than undefined behaviour, guarded by the writable
// flag on the writer's constructor and by a debug assertion on every store; and another
// process truncating the backing file below the mapped length turns a later touch into a
// SIGBUS, which no in-process check can prevent and which the 0600 file mode and the local
// trust boundary are the mitigation for.
unsafe impl Send for SegmentRegion {}
// SAFETY: as for `Send` above.
unsafe impl Sync for SegmentRegion {}

impl SegmentRegion {
    /// Allocates a zeroed region of exactly `bytes` bytes, aligned to [`REGION_ALIGNMENT`].
    ///
    /// `bytes` must be non-zero, a whole number of [`REGION_ALIGNMENT`]-byte blocks, and at
    /// most [`MAX_REGION_BYTES`]; anything else fails with the matching [`RegionError`]
    /// rather than being rounded. Every byte starts zero, which is what makes an
    /// unformatted region read as an unpublished header rather than as garbage.
    pub fn zeroed(bytes: usize) -> Result<Self, RegionError> {
        check_size(bytes)?;
        let mut blocks = Vec::new();
        blocks.resize_with(bytes / REGION_ALIGNMENT, || {
            AlignedBlock(UnsafeCell::new([0_u8; REGION_ALIGNMENT]))
        });
        let blocks = blocks.into_boxed_slice();
        let base = blocks
            .first()
            .map_or(core::ptr::NonNull::dangling().as_ptr(), |block| {
                block.0.get().cast::<u8>()
            });
        Ok(Self {
            backing: RegionBacking::Heap(blocks),
            base,
            size: bytes,
            writable: true,
            attachment: NEXT_ATTACHMENT.fetch_add(1, Ordering::Relaxed),
            path: None,
            creation: None,
        })
    }

    /// Creates the backing file at `path`, sizes it to `bytes` once, and maps it shared
    /// read-write.
    ///
    /// The file must not already exist: creation is exclusive, so a second writer cannot
    /// adopt a live segment's file. On Unix the file is created mode 0600 — owner
    /// read/write, nothing for group or world — which is the permission half of the local
    /// trust boundary; on other platforms the file inherits the process's default
    /// protection and the deployment supplies the equivalent restriction.
    ///
    /// `set_len` runs before the mapping exists, so the segment is never resized while
    /// mapped, and the whole mapping is then written with zeros so that every page is
    /// resident and dirty before the first publication: the update path takes no
    /// file-extension syscall and no first-touch allocation fault.
    ///
    /// The created file is kept for the region's life rather than dropped once the mapping
    /// exists, and its `fstat` identity with it ([`Self::creation_identity`]). `path` is a
    /// mutable name and this call is the only moment at which it provably resolves to this
    /// region's backing; every later question about which object that is — the doorbell
    /// probe's read-only mapping, a daemon's check of the descriptor it will serve attaches
    /// from — is answered against the retained object instead of by resolving the name a
    /// second time. The cost is one descriptor per created region.
    ///
    /// Fails with [`RegionError::Io`] carrying the failure kind, the size errors of
    /// [`Self::zeroed`], and [`RegionError::MappingMisaligned`] if the kernel returns an
    /// address below [`REGION_ALIGNMENT`].
    pub fn create_file(path: &Path, bytes: usize) -> Result<Self, RegionError> {
        Self::create_file_with_mode(path, bytes, 0o600)
    }

    /// [`Self::create_file`] at an explicit Unix permission mode.
    ///
    /// The mode is applied twice — once as the creation mode and once with an explicit
    /// permission set after the file exists — because the process umask silently strips bits
    /// from a creation mode, so a creation mode alone pins only an upper bound on the
    /// resulting permissions rather than the mode a caller asked for. Creation stays
    /// exclusive: an occupied path is refused as [`RegionError::Io`] with
    /// [`std::io::ErrorKind::AlreadyExists`] and the file already there is left untouched,
    /// never truncated or reopened. On a non-Unix platform the mode is ignored and the file
    /// inherits the process's default protection, exactly as before.
    pub(super) fn create_file_with_mode(
        path: &Path,
        bytes: usize,
        mode: u32,
    ) -> Result<Self, RegionError> {
        check_size(bytes)?;
        let mut options = OpenOptions::new();
        let _ = options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let _ = options.mode(mode);
        }
        let file = options.open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        }
        file.set_len(bytes as u64)?;
        let stat = file.metadata()?;
        // SAFETY: `file` was created exclusively by this call and sized before mapping, and
        // no other party holds it yet; the mapping is therefore of a stable length. The
        // residual hazard — another process truncating the file later — is documented on
        // the `Sync` impl above.
        let mut map = unsafe { MmapMut::map_mut(&file) }?;
        map.fill(0);
        let base = map.as_mut_ptr();
        Self::mapped(
            RegionBacking::Writable(map),
            base,
            bytes,
            true,
            Some(path.to_path_buf()),
            Some(CreationObject {
                identity: (stat.dev(), stat.ino()),
                file,
            }),
        )
    }

    /// Opens the backing file at `path` read-only and maps it shared read-only.
    ///
    /// A consumer never needs write access to any cell of this ABI, so a buggy or hostile
    /// reader cannot corrupt authoritative state at all rather than merely being
    /// discouraged from it. The mapped length is the file's length at open time; a file
    /// whose length is not a positive multiple of [`REGION_ALIGNMENT`] is refused here, and
    /// the header validator refuses everything else.
    pub fn open_file(path: &Path) -> Result<Self, RegionError> {
        let file = File::open(path)?;
        let bytes =
            usize::try_from(file.metadata()?.len()).map_err(|_| RegionError::SizeTooLarge)?;
        check_size(bytes)?;
        // SAFETY: the mapping is read-only and read-shared; see the `Sync` impl above for
        // the full access argument and for the truncation hazard this cannot exclude.
        let map = unsafe { Mmap::map(&file) }?;
        let base = map.as_ptr().cast_mut();
        Self::mapped(
            RegionBacking::ReadOnly(map),
            base,
            bytes,
            false,
            Some(path.to_path_buf()),
            None,
        )
    }

    /// Maps a segment file this process was handed as an already-open descriptor, read-only.
    ///
    /// The descriptor-transfer half of [`Self::open_file`]: a consumer that attached over the
    /// daemon's control channel never opens a path at all, so it can neither race a rename nor
    /// be pointed at a file it was not given — possession of the descriptor is the
    /// authorization (`docs/notes/shared-memory-model.md` §4.2). The mapping is read-only for
    /// the same reason every consumer mapping is: no cell of this ABI is a consumer's to
    /// write.
    ///
    /// `descriptor` must be open for reading; it is consumed and closed once the mapping
    /// exists, which the mapping itself does not need. The region carries no backing path,
    /// because a transferred descriptor names no path this process may assume — which is why
    /// a segment declaring a sibling doorbell page is attached with
    /// [`super::SegmentReader::attach_with_doorbell`] and its page's own descriptor, never by
    /// resolving a name.
    ///
    /// Fails with the size errors of [`Self::zeroed`] for a file whose length is not a
    /// positive multiple of [`REGION_ALIGNMENT`], and [`RegionError::Io`] for a descriptor
    /// that cannot be stat'd or mapped.
    pub fn open_read_only_from_fd(descriptor: std::os::fd::OwnedFd) -> Result<Self, RegionError> {
        let file = File::from(descriptor);
        let bytes =
            usize::try_from(file.metadata()?.len()).map_err(|_| RegionError::SizeTooLarge)?;
        check_size(bytes)?;
        // SAFETY: the mapping is read-only and read-shared; see the `Sync` impl above for the
        // full access argument and for the truncation hazard this cannot exclude.
        let map = unsafe { Mmap::map(&file) }?;
        let base = map.as_ptr().cast_mut();
        Self::mapped(RegionBacking::ReadOnly(map), base, bytes, false, None, None)
    }

    /// Maps a sibling doorbell page this process was handed as an already-open descriptor,
    /// read-write.
    ///
    /// The descriptor-transfer half of [`Self::open_page_read_write`], and read-write for that
    /// method's reason alone: the platform's wait primitive requires a mapping that accepts
    /// writes, never because a consumer stores anything through it. `descriptor` must be open
    /// for reading and writing.
    pub(super) fn open_page_read_write_from_fd(
        descriptor: std::os::fd::OwnedFd,
    ) -> Result<Self, RegionError> {
        let file = File::from(descriptor);
        let bytes =
            usize::try_from(file.metadata()?.len()).map_err(|_| RegionError::SizeTooLarge)?;
        check_size(bytes)?;
        // SAFETY: a shared mapping of a file this process holds a read/write descriptor for,
        // reached only through `RegionCells` exactly like every other mapping this type
        // produces; see the `Sync` impl above for the full access argument.
        let mut map = unsafe { MmapMut::map_mut(&file) }?;
        let base = map.as_mut_ptr();
        Self::mapped(RegionBacking::Writable(map), base, bytes, true, None, None)
    }

    fn mapped(
        backing: RegionBacking,
        base: *mut u8,
        size: usize,
        writable: bool,
        path: Option<std::path::PathBuf>,
        creation: Option<CreationObject>,
    ) -> Result<Self, RegionError> {
        if !base.addr().is_multiple_of(REGION_ALIGNMENT) {
            return Err(RegionError::MappingMisaligned);
        }
        Ok(Self {
            backing,
            base,
            size,
            writable,
            attachment: NEXT_ATTACHMENT.fetch_add(1, Ordering::Relaxed),
            path,
            creation,
        })
    }

    /// Opens an existing sibling doorbell page at `path` and maps it shared read-write.
    ///
    /// This never creates the page — [`super::SegmentWriter::create`] and its doorbell
    /// placement own that — it only attaches to one that already exists. A consumer never
    /// stores anything meaningful into it: only the platform's wait primitive requires the
    /// mapping itself to accept writes, which is the whole reason the page exists instead of
    /// a read-only mapping like every other consumer view of this ABI.
    ///
    /// Fails with [`RegionError::Io`] when the file cannot be opened, and the size errors of
    /// [`Self::zeroed`] if its length is not a positive multiple of [`REGION_ALIGNMENT`].
    pub(super) fn open_page_read_write(path: &Path) -> Result<Self, RegionError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let bytes =
            usize::try_from(file.metadata()?.len()).map_err(|_| RegionError::SizeTooLarge)?;
        check_size(bytes)?;
        // SAFETY: a shared mapping of a file this process just opened for read/write,
        // reached only through `RegionCells` exactly like every other mapping this type
        // produces; see the `Sync` impl above for the full access argument.
        let mut map = unsafe { MmapMut::map_mut(&file) }?;
        let base = map.as_mut_ptr();
        Self::mapped(
            RegionBacking::Writable(map),
            base,
            bytes,
            true,
            Some(path.to_path_buf()),
            None,
        )
    }

    /// The file this region maps, or `None` for a heap allocation.
    ///
    /// It is the discriminator between a region a second process can map and one that lives
    /// only in this address space, which is what decides whether the doorbell needs a sibling
    /// page: a heap region's readers share the writer's own mapping, so its header word is
    /// always waitable.
    pub(super) fn backing_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The open file description this region was created as, or `None` for a region this
    /// process did not create.
    ///
    /// The anchor every identity question about a created region is answered against: a
    /// mapping taken from a descriptor duplicated from this one is a mapping of *this*
    /// object, whatever the name it was created under resolves to by then.
    pub(super) fn creation_file(&self) -> Option<&File> {
        self.creation.as_ref().map(|creation| &creation.file)
    }

    /// The device and inode of [`Self::creation_file`], `fstat`ed while the exclusive create
    /// still guaranteed what the name resolved to, or `None` for a region this process did
    /// not create.
    ///
    /// A descriptor obtained any other way — by re-opening the name this region was created
    /// under, say — names this region's backing exactly when its own `fstat` matches this
    /// pair, which is a comparison against the object rather than between two resolutions of
    /// a name that anything running as this user may have changed in between.
    pub fn creation_identity(&self) -> Option<(u64, u64)> {
        self.creation.as_ref().map(|creation| creation.identity)
    }

    /// The region's size in bytes: always a positive multiple of [`REGION_ALIGNMENT`].
    pub fn size_bytes(&self) -> usize {
        self.size
    }

    /// This region's process-local attachment identity, unique among the regions this
    /// process has constructed.
    ///
    /// It is what lets a routing handle refuse a region it did not come from, even when two
    /// regions declare the same `daemon_instance_id` and `segment_generation` — which the
    /// header alone cannot distinguish. It never leaves the process and is never written
    /// into the segment.
    pub fn attachment_id(&self) -> u64 {
        self.attachment
    }

    /// Whether this region's backing accepts stores. A read-only file mapping does not, and
    /// [`super::SegmentWriter::create`] refuses one.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// The cell view of this region, from which records are carved.
    pub(super) fn cells(&self) -> RegionCells<'_> {
        RegionCells {
            base: self.base,
            size: self.size,
            writable: self.writable,
            region: PhantomData,
        }
    }
}

fn check_size(bytes: usize) -> Result<(), RegionError> {
    if bytes == 0 {
        return Err(RegionError::SizeZero);
    }
    if !bytes.is_multiple_of(REGION_ALIGNMENT) {
        return Err(RegionError::SizeNotBlockAligned);
    }
    if bytes > MAX_REGION_BYTES {
        return Err(RegionError::SizeTooLarge);
    }
    Ok(())
}

/// A bounds-checked view of one whole region.
#[derive(Clone, Copy)]
pub(super) struct RegionCells<'a> {
    base: *mut u8,
    size: usize,
    writable: bool,
    region: PhantomData<&'a SegmentRegion>,
}

impl<'a> RegionCells<'a> {
    pub(super) fn size_bytes(self) -> usize {
        self.size
    }

    /// Carves the record occupying `offset..offset + size`.
    ///
    /// Returns `None` when the range does not fit the region or `offset` is not a multiple
    /// of 8, which is the alignment every record base needs for its widest cell. This is
    /// the one bounds check per record: the accessors on [`Record`] then work at constant
    /// sub-offsets inside a range already proven to fit.
    pub(super) fn record(self, offset: usize, size: usize) -> Option<Record<'a>> {
        if !offset.is_multiple_of(8) || size > self.size || offset > self.size - size {
            return None;
        }
        // SAFETY: `offset + size <= self.size` was just checked, so the result points into
        // the region's allocation and `size` bytes follow it.
        let base = unsafe { self.base.add(offset) };
        Some(Record {
            base,
            size,
            writable: self.writable,
            region: PhantomData,
        })
    }
}

/// One record of a region, proven to fit at construction.
///
/// Accessors take an offset inside the record and read or write it at one fixed width.
/// Those offsets are module constants, never reader or venue input, so the assertions below
/// guard a layout programming error and cannot be provoked from outside this crate.
#[derive(Clone, Copy)]
pub(super) struct Record<'a> {
    base: *mut u8,
    size: usize,
    writable: bool,
    region: PhantomData<&'a SegmentRegion>,
}

impl<'a> Record<'a> {
    fn ptr(self, offset: usize, width: usize) -> *mut u8 {
        assert!(
            offset.is_multiple_of(width) && width <= self.size && offset <= self.size - width,
            "record cell out of range"
        );
        // SAFETY: the record covers `self.size` bytes and the assertion proved
        // `offset + width <= self.size`, so the result stays inside that range. The record
        // base is 8-aligned and `offset` is a multiple of `width`, so the result is
        // naturally aligned for a `width`-byte atomic.
        unsafe { self.base.add(offset) }
    }

    /// The record occupying `offset..offset + size` inside this one, for a nested cell such
    /// as a level or a decimal.
    pub(super) fn sub(self, offset: usize, size: usize) -> Record<'a> {
        assert!(
            offset.is_multiple_of(8) && size <= self.size && offset <= self.size - size,
            "nested record out of range"
        );
        // SAFETY: the assertion proved `offset + size <= self.size`, so the nested record
        // stays inside this one, and `offset` is a multiple of 8 so its base keeps the
        // 8-byte alignment every cell width needs.
        let base = unsafe { self.base.add(offset) };
        Record {
            base,
            size,
            writable: self.writable,
            region: PhantomData,
        }
    }

    fn assert_writable(self) {
        debug_assert!(
            self.writable,
            "store into a read-only region: a store through a read-only mapping is a fault, \
             and only the writer may store"
        );
    }

    /// The `synchronizing` 64-bit cell at `offset`.
    pub(super) fn sync(self, offset: usize) -> SyncCell<'a> {
        let ptr = self.ptr(offset, 8);
        // SAFETY: `ptr` is in bounds and 8-aligned, and this byte range is only ever
        // reached as a 64-bit atomic, so the reference names a valid `AtomicU64` that lives
        // as long as the region borrow.
        SyncCell {
            cell: unsafe { &*ptr.cast::<AtomicU64>() },
            writable: self.writable,
        }
    }

    /// The even-odd sequence counter at `offset`.
    pub(super) fn seq(self, offset: usize) -> SeqCell<'a> {
        let ptr = self.ptr(offset, 8);
        // SAFETY: `ptr` is in bounds and 8-aligned, and this byte range is only ever
        // reached as a 64-bit atomic.
        SeqCell {
            cell: unsafe { &*ptr.cast::<AtomicU64>() },
            writable: self.writable,
        }
    }

    /// The `relaxed_data_word` 64-bit cell at `offset`.
    pub(super) fn word64(self, offset: usize) -> RelaxedWord64<'a> {
        let ptr = self.ptr(offset, 8);
        // SAFETY: as for `sync` above; this byte range is only ever reached as a 64-bit
        // atomic.
        RelaxedWord64 {
            cell: unsafe { &*ptr.cast::<AtomicU64>() },
            writable: self.writable,
        }
    }

    /// The `relaxed_data_word` 32-bit cell at `offset`.
    pub(super) fn word32(self, offset: usize) -> RelaxedWord32<'a> {
        let ptr = self.ptr(offset, 4);
        // SAFETY: `ptr` is in bounds and 4-aligned, and this byte range is only ever
        // reached as a 32-bit atomic.
        RelaxedWord32 {
            cell: unsafe { &*ptr.cast::<AtomicU32>() },
            writable: self.writable,
        }
    }

    /// Acquire-loads the write-once gate at `gate_offset` and, when it holds a published
    /// value, returns the witness that unlocks this record's `write_once_published` bytes.
    ///
    /// `None` means the record has not been published — either untouched (zero) or claimed
    /// by a formatter that has not finished ([`GATE_CLAIMED`]) — and the bytes must not be
    /// read.
    pub(super) fn published(self, gate_offset: usize) -> Option<PublishedWitness<'a>> {
        let gate = self.sync(gate_offset).acquire_load();
        (gate != 0 && gate != GATE_CLAIMED).then_some(PublishedWitness { record: self, gate })
    }

    /// Writes this record's whole write-once payload and publishes it with one release
    /// store of the gate at `gate_offset`.
    ///
    /// The record is **claimed atomically first**: the gate is compare-exchanged from zero
    /// to [`GATE_CLAIMED`], and only the thread that wins that exchange performs any
    /// payload write. A plain read-then-write test would let two threads holding the same
    /// region both pass and then race on non-atomic bytes, which is a data race in safe
    /// code; the exchange makes the claim the single serialization point. A loser writes
    /// nothing and gets `false`.
    ///
    /// Payload and release are otherwise inseparable on purpose: the plain writes cannot be
    /// ordered wrongly with respect to the gate, and no caller can write a write-once field
    /// after publication. Returns `false` without writing anything when the record is
    /// already claimed or published — a record is published exactly once per segment
    /// generation — and `true` after publishing.
    ///
    /// `gate_value` must be neither zero nor [`GATE_CLAIMED`], or the witness could never
    /// be obtained; that is asserted, and every caller passes a constant.
    pub(super) fn publish_write_once(
        self,
        gate_offset: usize,
        gate_value: u64,
        fields: &[WriteOnceField<'_>],
    ) -> bool {
        self.assert_writable();
        assert!(
            gate_value != 0 && gate_value != GATE_CLAIMED,
            "a write-once gate value must be publishable"
        );
        let gate = self.sync(gate_offset);
        if !gate.claim() {
            return false;
        }
        for field in fields {
            match field {
                WriteOnceField::Bytes { offset, value } => {
                    assert!(
                        value.len() <= self.size && *offset <= self.size - value.len(),
                        "write-once bytes out of range"
                    );
                    // SAFETY: the assertion proved the destination range lies inside this
                    // record, `value` is a distinct borrow so the ranges cannot overlap,
                    // and this write happens before the gate's release store below, so no
                    // reader can be reading these bytes concurrently.
                    unsafe {
                        core::ptr::copy_nonoverlapping(
                            value.as_ptr(),
                            self.base.add(*offset),
                            value.len(),
                        );
                    }
                }
                WriteOnceField::Word32 { offset, value } => {
                    let ptr = self.ptr(*offset, 4);
                    // SAFETY: `ptr` is in bounds and 4-aligned, and this write precedes the
                    // gate's release store, so no reader can observe it concurrently.
                    unsafe { ptr.cast::<u32>().write(value.to_le()) };
                }
                WriteOnceField::Word64 { offset, value } => {
                    let ptr = self.ptr(*offset, 8);
                    // SAFETY: `ptr` is in bounds and 8-aligned, and this write precedes the
                    // gate's release store, so no reader can observe it concurrently.
                    unsafe { ptr.cast::<u64>().write(value.to_le()) };
                }
            }
        }
        gate.release_store(gate_value);
        true
    }
}

/// One field of a record's write-once payload.
///
/// Offsets are byte offsets inside the record; word values are stored little-endian, which
/// is the byte order every multi-word field of this ABI is read back in.
pub(super) enum WriteOnceField<'a> {
    Bytes { offset: usize, value: &'a [u8] },
    Word32 { offset: usize, value: u32 },
    Word64 { offset: usize, value: u64 },
}

/// Proof that a record's write-once gate was acquire-loaded and observed non-zero.
///
/// It is the only key to that record's `write_once_published` bytes, so the class rule —
/// read them only after an acquire load of the governing gate saw a non-zero value — is a
/// precondition the borrow checker holds rather than a convention a caller may forget.
#[derive(Clone, Copy)]
pub(super) struct PublishedWitness<'a> {
    record: Record<'a>,
    gate: u64,
}

impl<'a> PublishedWitness<'a> {
    /// The non-zero gate value this witness was produced by.
    pub(super) fn gate(self) -> u64 {
        self.gate
    }

    /// The write-once bytes at `offset..offset + len`.
    pub(super) fn bytes(self, offset: usize, len: usize) -> &'a [u8] {
        assert!(
            len <= self.record.size && offset <= self.record.size - len,
            "write-once bytes out of range"
        );
        // SAFETY: the assertion proved the range lies inside the record. These bytes are
        // `write_once_published`: they were written before the release store of the gate
        // this witness acquire-loaded, and are never written again while the segment
        // generation lives, so no write can be concurrent with this borrow.
        unsafe { core::slice::from_raw_parts(self.record.base.add(offset).cast_const(), len) }
    }

    /// The write-once little-endian 32-bit word at `offset`.
    pub(super) fn word32(self, offset: usize) -> u32 {
        let ptr = self.record.ptr(offset, 4);
        // SAFETY: `ptr` is in bounds and 4-aligned, and the word is write-once published
        // behind the gate this witness acquire-loaded, so no write is concurrent with it.
        u32::from_le(unsafe { ptr.cast::<u32>().read() })
    }

    /// The write-once little-endian 64-bit word at `offset`.
    pub(super) fn word64(self, offset: usize) -> u64 {
        let ptr = self.record.ptr(offset, 8);
        // SAFETY: `ptr` is in bounds and 8-aligned, and the word is write-once published
        // behind the gate this witness acquire-loaded, so no write is concurrent with it.
        u64::from_le(unsafe { ptr.cast::<u64>().read() })
    }
}

/// A `synchronizing` 64-bit cell.
///
/// Exactly two operations exist, and they are the two the class rule allows. There is no
/// plain load, no relaxed access, no `Deref`, and no accessor yielding the underlying
/// atomic, so the rule that a synchronizing cell is never plainly accessed cannot be broken
/// by a caller — including a validator, which is where it was broken before.
#[derive(Clone, Copy)]
pub(super) struct SyncCell<'a> {
    cell: &'a AtomicU64,
    writable: bool,
}

impl SyncCell<'_> {
    /// Loads the cell with `Acquire`, so every relaxed data write the paired release store
    /// ordered is visible afterwards. The unit is the cell's own counter value.
    pub(super) fn acquire_load(self) -> u64 {
        self.cell.load(Ordering::Acquire)
    }
    /// Stores `value` with `Release`, publishing every relaxed data write made before it.
    ///
    /// Debug builds assert the region is writable, so a store added to a read path is
    /// caught by the test suite rather than by a fault in a consumer process.
    pub(super) fn release_store(self, value: u64) {
        debug_assert!(self.writable, "release store into a read-only region");
        self.cell.store(value, Ordering::Release);
    }
    /// Claims an unpublished gate for this thread, returning whether the claim was won.
    ///
    /// Exchanges zero for [`GATE_CLAIMED`]. The success ordering is `Acquire`, so nothing
    /// the winner does afterwards — in particular no non-atomic payload write — can be
    /// reordered before the claim, and every loser observes the claim before it gives up.
    /// This is the only mutating operation on a synchronizing cell that is not a plain
    /// release store, and it exists because a read-then-write claim is a data race.
    fn claim(self) -> bool {
        debug_assert!(self.writable, "claim of a gate in a read-only region");
        self.cell
            .compare_exchange(0, GATE_CLAIMED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }
}

/// The even-odd sequence counter of a seqlock-published record.
///
/// A seqlock is not expressible with one-way `Release`/`Acquire` stores on the counter
/// alone: a release store orders only what precedes it, so the *opening* odd store cannot
/// hold back the data stores that follow it; symmetrically an acquire load orders only what
/// follows it, so the *closing* load cannot hold back the data loads that precede it. A
/// writer's data stores could therefore sink below the closing even store, and a reader's
/// data loads could sink below its recheck — either way a reader can observe one unchanged
/// even value around data from a different publication.
///
/// The construction below is the standard fenced one. Writer: relaxed store of the odd
/// value, `Release` fence, relaxed data stores, `Release` store of the next even value.
/// Reader: `Acquire` load, reject odd, relaxed data loads, `Acquire` fence, relaxed reload,
/// accept only if unchanged. The fences are the two-way barriers the counter's own accesses
/// cannot provide, and they live inside this type so no caller can assemble the sequence
/// without them.
#[derive(Clone, Copy)]
pub(super) struct SeqCell<'a> {
    cell: &'a AtomicU64,
    writable: bool,
}

/// Proof that a writer opened a publication interval on a [`SeqCell`].
#[must_use]
pub(super) struct WriteInterval {
    in_flight: u64,
}

/// Proof that a reader opened a read interval on a [`SeqCell`], carrying the even value it
/// must observe again for the copy to be accepted.
#[must_use]
#[derive(Clone, Copy)]
pub(super) struct ReadInterval {
    opened: u64,
}

/// What a reader found when it opened a [`SeqCell`].
pub(super) enum SeqOpen {
    /// The record has never been published.
    Unpublished,
    /// A publication is in flight; the value is the odd counter observed.
    InFlight(u64),
    /// The record is stable and may be copied under this interval.
    Stable(ReadInterval),
}

impl SeqCell<'_> {
    /// The counter's current value, for the sole writer's own arithmetic.
    ///
    /// A relaxed load is sufficient and correct here: no other thread writes this cell, so
    /// the writer's own last store is the value it reads.
    pub(super) fn writer_current(self) -> u64 {
        debug_assert!(self.writable, "writer read of a read-only region's counter");
        self.cell.load(Ordering::Relaxed)
    }

    /// Marks the record in flight with `in_flight`, which must be odd.
    ///
    /// The store is `Relaxed` and is followed by a `Release` fence, so every data store the
    /// caller makes afterwards is ordered after the odd value became visible, which a
    /// release store on the counter alone would not achieve.
    pub(super) fn open_write(self, in_flight: u64) -> WriteInterval {
        debug_assert!(self.writable, "publication into a read-only region");
        debug_assert!(in_flight % 2 == 1, "an in-flight counter value must be odd");
        self.cell.store(in_flight, Ordering::Relaxed);
        fence(Ordering::Release);
        WriteInterval { in_flight }
    }

    /// Closes the interval with `stable`, which must be even and greater than the in-flight
    /// value. The `Release` store publishes every data store made inside the interval.
    pub(super) fn close_write(self, interval: WriteInterval, stable: u64) {
        debug_assert!(self.writable, "publication into a read-only region");
        debug_assert!(
            stable.is_multiple_of(2) && stable > interval.in_flight,
            "stable follows odd"
        );
        self.cell.store(stable, Ordering::Release);
    }

    /// Opens a read interval: an `Acquire` load that classifies the counter.
    pub(super) fn open_read(self) -> SeqOpen {
        let opened = self.cell.load(Ordering::Acquire);
        if opened == 0 {
            SeqOpen::Unpublished
        } else if opened % 2 == 1 {
            SeqOpen::InFlight(opened)
        } else {
            SeqOpen::Stable(ReadInterval { opened })
        }
    }

    /// Whether the copy taken under `interval` may be accepted.
    ///
    /// The `Acquire` fence comes first so no data load the caller made can sink past the
    /// reload, which an acquire load on the counter alone would not prevent; the reload
    /// itself is then `Relaxed`, because the fence already supplies the ordering.
    pub(super) fn close_read(self, interval: ReadInterval) -> bool {
        fence(Ordering::Acquire);
        self.cell.load(Ordering::Relaxed) == interval.opened
    }
}

/// A `relaxed_data_word` 64-bit cell.
///
/// Both operations are `Relaxed`: ordering comes from the record's synchronizing cell,
/// never from a data access, and this type offers no way to pretend otherwise.
#[derive(Clone, Copy)]
pub(super) struct RelaxedWord64<'a> {
    cell: &'a AtomicU64,
    writable: bool,
}

impl RelaxedWord64<'_> {
    pub(super) fn relaxed_load(self) -> u64 {
        self.cell.load(Ordering::Relaxed)
    }
    /// Debug builds assert the region is writable; see [`SyncCell::release_store`].
    pub(super) fn relaxed_store(self, value: u64) {
        debug_assert!(self.writable, "relaxed store into a read-only region");
        self.cell.store(value, Ordering::Relaxed);
    }
}

/// A `relaxed_data_word` 32-bit cell. As [`RelaxedWord64`], at 32 bits.
#[derive(Clone, Copy)]
pub(super) struct RelaxedWord32<'a> {
    cell: &'a AtomicU32,
    writable: bool,
}

impl RelaxedWord32<'_> {
    pub(super) fn relaxed_load(self) -> u32 {
        self.cell.load(Ordering::Relaxed)
    }
    /// Debug builds assert the region is writable; see [`SyncCell::release_store`].
    pub(super) fn relaxed_store(self, value: u32) {
        debug_assert!(self.writable, "relaxed store into a read-only region");
        self.cell.store(value, Ordering::Relaxed);
    }

    /// This cell's address, for the operating system's wait and wake primitives alone.
    ///
    /// The primitives compare and block on a 4-byte cell the kernel reads for itself, so
    /// they need an address rather than a value, and no cell operation can express that.
    /// [`WakeAddress`] is the whole of what crosses: it carries no load, no store and no
    /// dereference, so the class rule of `docs/notes/shared-memory-model.md` §1 — no public
    /// path hands out a reference to a mutable cell — still holds, and the only code that
    /// can consume one is [`super::doorbell`].
    pub(super) fn wake_address(self) -> WakeAddress {
        WakeAddress(core::ptr::from_ref(self.cell).cast_mut().cast::<u32>())
    }
}

/// The address of one 32-bit cell, usable only as an operand of a platform wait or wake.
///
/// It is deliberately opaque: there is no accessor yielding the pointer outside this module
/// family, no deref, and no way to read or write the cell through it. A [`WakeAddress`] is
/// copyable and carries no lifetime because a wait primitive may hold it across a park while
/// the region borrow it came from is still alive at the caller; the region outliving the wait
/// is the caller's obligation, stated on [`super::doorbell::wait`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WakeAddress(*mut u32);

// SAFETY: the address names a cell of a `SegmentRegion`, whose `Send`/`Sync` argument above
// already covers concurrent access to it from any thread; this type adds no access of its own
// beyond handing the address to a kernel wait or wake.
unsafe impl Send for WakeAddress {}
// SAFETY: as for `Send` above.
unsafe impl Sync for WakeAddress {}

impl WakeAddress {
    /// The raw pointer, for the platform primitives in [`super::doorbell`] only.
    pub(super) fn as_ptr(self) -> *mut u32 {
        self.0
    }
}
