//! Latest-state publication over a shared region: one writer formats an aligned region and
//! publishes each market's [`crate::PublishedBook`] into a fixed state slot under an
//! even-odd seqlock; any number of readers validate the header, resolve a venue-native
//! identity to a slot, and take a consistent snapshot without ever blocking the writer.
//!
//! The cell classes, the publication protocol and why the seqlock is the only candidate are
//! `docs/notes/shared-memory-model.md`. This module implements that note over an ordinary
//! in-process region: [`SegmentRegion`] is the seam a mapped backing attaches to later, and
//! nothing above it knows which backing it has.
//!
//! Latest state and retained mutations are the two coordinated outputs of one book. Each
//! market owns one bounded event ring; it wraps, the writer never blocks and never skips a
//! position, and a consumer the writer outran is told so explicitly rather than handed
//! partial history. [`SegmentReader::attach_stream`] joins the two: a snapshot and the
//! stream position it stops at, taken from one accepted seqlock interval.
//!
//! A consumer that has caught up need not poll on a timer. Every publication round advances a
//! doorbell word the platform's wait primitive parks on, appends one entry to a bounded
//! dirty index naming which market moved, and — once per round — posts a wake. Where the
//! doorbell lives is decided by the writer at creation and declared in the header's feature
//! word, because not every platform will wait on the read-only mapping a consumer holds.
//!
//! A consumer reaches a mapped segment by descriptor transfer rather than by opening a path:
//! [`channel`] is the authenticated local attachment channel `docs/notes/shared-memory-model.md`
//! §4.2 asks for, and [`SegmentRegion::open_read_only_from_fd`] and
//! [`SegmentReader::attach_with_doorbell`] are what a received transfer is turned into.

mod cell;
mod channel;
pub(crate) mod codec;
mod doorbell;
mod layout;
mod reader;
mod writer;

pub use cell::{MAX_REGION_BYTES, REGION_ALIGNMENT, RegionError, SegmentRegion};
pub use channel::{
    ChannelError, MAX_TRANSFERRED_DESCRIPTORS, own_euid, peer_euid, recv_with_fds, send_with_fds,
};
pub use layout::{
    ABI_VERSION, DEFAULT_DIRTY_CAPACITY, DEFAULT_EVENT_CAPACITY, FEATURE_DOORBELL_IN_HEADER,
    FEATURE_DOORBELL_PAGE, FEATURE_EVENT_RING_WRAPS, LayoutError, MAX_LEVEL_CAPACITY, MarketHandle,
    NO_STATE_SLOT, SegmentBinding, SegmentFault, SegmentGeometry, SegmentLayout, validate,
};
pub(crate) use layout::{RES_DATE_CAPACITY, RES_OUTCOME_CAPACITY, RES_TYPE_CAPACITY};
pub use reader::{
    BookSnapshot, DirtyCursor, DirtyPoll, EventPoll, EventStream, MAX_READ_ATTEMPTS, MutationEvent,
    PublicationOrigin, ReadFault, ResolutionEvent, ResolutionPublication, RetainedEvent,
    SegmentReader, StateCursor, StreamFault, WaitFault, WaitOutcome,
};
pub use writer::{DoorbellPlacement, SegmentConfig, SegmentWriter, WriterError};

#[cfg(test)]
mod tests {
    use super::layout::{
        DIRECTORY_ENTRY_BYTES, DIRTY_BOOK_REVISION, DIRTY_DIRECTORY_INDEX, DIRTY_POSITION,
        DIRTY_SEQUENCE, DIRTY_SLOT_BYTES, ENT_REVISION, ENT_SLOT_INDEX, EVT_ARRIVAL_TIME,
        HDR_DOORBELL, HEADER_BYTES, NATIVE_FAMILY_CAPACITY, NO_STATE_SLOT, SLOT_ARRIVAL_TIME,
        SLOT_REVISION, TRAILER_BYTES,
    };
    use super::*;
    use crate::{MarketRef, NativeIdentifierKind, NativeMarketKey, Venue};
    use core::time::Duration;
    use std::sync::Arc;

    /// The safety net every doorbell test in this module shares: long enough that a loaded
    /// host never trips it, short enough that a real failure is a failure rather than a hung
    /// suite.
    const WAKE_DEADLINE: Duration = Duration::from_secs(10);

    fn layout() -> SegmentLayout {
        SegmentLayout::new(4, 4, 8, 8, 16).expect("test layout is valid")
    }

    fn market(slug: &str) -> MarketRef {
        MarketRef::new(
            Venue::new("limitless").expect("venue"),
            NativeMarketKey::new(NativeIdentifierKind::slug(), slug).expect("key"),
        )
    }

    fn formatted() -> (Arc<SegmentRegion>, SegmentWriter) {
        let region =
            Arc::new(SegmentRegion::zeroed(layout().region_size()).expect("region allocates"));
        let writer = SegmentWriter::create(
            Arc::clone(&region),
            SegmentConfig::new(layout(), 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210, 7),
        )
        .expect("segment formats");
        (region, writer)
    }

    fn scribble(region: &SegmentRegion, pattern: u8) {
        let cells = region.cells();
        let trailer_offset = region.size_bytes() - TRAILER_BYTES;
        let body = cells
            .record(HEADER_BYTES, trailer_offset - HEADER_BYTES)
            .expect("body record");
        let word = u64::from_le_bytes([pattern; 8]);
        for offset in (0..trailer_offset - HEADER_BYTES).step_by(8) {
            body.word64(offset).relaxed_store(word);
        }
    }

    #[test]
    fn shm_validator_reads_only_the_header_and_trailer() {
        let (region, _writer) = formatted();
        let clean = validate(&region).expect("clean segment validates");
        for pattern in [0x00, 0xaa, 0x55, 0xff] {
            scribble(&region, pattern);
            assert_eq!(validate(&region), Ok(clean));
        }
    }

    #[test]
    fn shm_validator_refuses_an_unformatted_or_hostile_header() {
        let region = SegmentRegion::zeroed(layout().region_size()).expect("region allocates");
        assert_eq!(validate(&region), Err(SegmentFault::HeaderUnpublished));

        let small = SegmentRegion::zeroed(HEADER_BYTES).expect("small region allocates");
        assert_eq!(
            validate(&small),
            Err(SegmentFault::RegionTooSmall { size: HEADER_BYTES })
        );

        let (region, _writer) = formatted();
        let header = super::layout::header_record(region.cells()).expect("header record");
        header
            .word64(super::layout::HDR_MAGIC)
            .relaxed_store(0xdead_beef_dead_beef);
        assert_eq!(
            validate(&region),
            Err(SegmentFault::MagicMismatch {
                found: 0xdead_beef_dead_beef
            })
        );
    }

    #[test]
    fn shm_validator_refuses_a_geometry_that_does_not_match_its_capacities() {
        let (region, _writer) = formatted();
        let header = super::layout::header_record(region.cells()).expect("header record");
        header
            .word32(super::layout::HDR_DIRECTORY_STRIDE)
            .relaxed_store(DIRECTORY_ENTRY_BYTES as u32 + 8);
        assert_eq!(validate(&region), Err(SegmentFault::GeometryMismatch));
    }

    #[test]
    fn shm_writer_refuses_to_adopt_a_formatted_segment() {
        let (region, _writer) = formatted();
        let second = SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout(), 1, 8));
        assert_eq!(second.err(), Some(WriterError::SegmentAlreadyFormatted));
    }

    /// A writer that stopped between its odd and even stores reads as unavailable.
    ///
    /// The odd value is stored through the one-way `sync` accessor rather than through
    /// `seq`, because `SeqCell` deliberately offers no way to leave an interval open — that
    /// is the state only a crash produces. It is also the one live instance of the
    /// wrong-class access the note's §1 records as a residual, and it is confined to this
    /// test.
    #[test]
    fn shm_reader_reports_a_writer_that_stopped_mid_publication() {
        let (region, mut writer) = formatted();
        let handle = writer.install(&market("btc-up-or-down")).expect("install");
        let reader = SegmentReader::attach(Arc::clone(&region)).expect("attach");
        assert_eq!(reader.read(handle), Err(ReadFault::NoPublishedState));

        let slot =
            super::layout::state_slot_record(region.cells(), layout(), 0).expect("slot record");
        slot.sync(SLOT_REVISION).release_store(3);
        assert_eq!(
            reader.read(handle),
            Err(ReadFault::WriterStalled { slot_revision: 3 })
        );
    }

    #[test]
    fn shm_reader_treats_the_reserved_slot_index_as_absence() {
        let (region, _writer) = formatted();
        let entry =
            super::layout::directory_record(region.cells(), layout(), 1).expect("entry record");
        entry.word32(ENT_SLOT_INDEX).relaxed_store(NO_STATE_SLOT);
        entry.sync(ENT_REVISION).release_store(1);
        let reader = SegmentReader::attach(Arc::clone(&region)).expect("attach");
        let binding = reader.binding();
        assert_eq!(
            reader.read(MarketHandle::new(binding, 1, NO_STATE_SLOT)),
            Err(ReadFault::NoPublishedState)
        );
        assert_eq!(
            reader.read(MarketHandle::new(binding, 1, 0)),
            Err(ReadFault::NoPublishedState)
        );
    }

    #[test]
    fn shm_discriminants_round_trip_and_reject_unknown_words() {
        use crate::{
            AuthorityReason as Reason, AuthorityState as Authority, ContinuityReason as Break,
            Derivation, MutationContinuity as Continuity, Origin, Representation, Side,
        };
        for state in [
            Authority::Unsubscribed,
            Authority::Subscribing,
            Authority::Synchronizing,
            Authority::Live,
            Authority::Recovering,
            Authority::Stale(Reason::Gap),
            Authority::Stale(Reason::Disconnect),
            Authority::Stale(Reason::SubscriptionLost),
            Authority::Stale(Reason::LocalLoss),
            Authority::Stale(Reason::OrderingUnknown),
            Authority::Stale(Reason::Overload),
            Authority::Stale(Reason::ReplicaDivergence),
            Authority::Stale(Reason::RecoveryBaseUnavailable),
        ] {
            let (word, reason) = super::codec::authority_words(&state);
            assert_eq!(super::codec::authority_state(word, reason), Some(state));
        }
        assert_eq!(super::codec::authority_state(0, 0), None);
        assert_eq!(super::codec::authority_state(4, 1), None);

        for reason in [
            Break::Overrun,
            Break::Gap,
            Break::LocalLoss,
            Break::Reconnect,
            Break::RecoveryBase,
            Break::SyncDivergence,
        ] {
            let continuity = Continuity::Lost { epoch: 9, reason };
            let (kind, word, epoch, position) = super::codec::continuity_words(&continuity);
            assert_eq!(
                super::codec::continuity(kind, word, epoch, position),
                Some(continuity)
            );
        }
        let intact = Continuity::Intact {
            epoch: 2,
            next_position: 41,
        };
        let (kind, word, epoch, position) = super::codec::continuity_words(&intact);
        assert_eq!(
            super::codec::continuity(kind, word, epoch, position),
            Some(intact)
        );
        assert_eq!(super::codec::continuity(1, 3, 0, 0), None);
        assert_eq!(super::codec::continuity(2, 1, 0, 5), None);

        for origin in [
            Origin::SourceReported,
            Origin::NormalizedFromSource,
            Origin::LocallyDerived(Derivation::SnapshotDiff),
        ] {
            let (word, derivation) = super::codec::origin_words(&origin);
            assert_eq!(super::codec::origin(word, derivation), Some(origin));
        }
        assert_eq!(super::codec::origin(1, 1), None);

        for representation in [Representation::VenueNative, Representation::Normalized] {
            let word = super::codec::representation_word(&representation);
            assert_eq!(super::codec::representation(word), Some(representation));
        }
        assert_eq!(super::codec::representation(0), None);

        for side in [Side::Bid, Side::Ask] {
            assert_eq!(
                super::codec::side(super::codec::side_word(side)),
                Some(side)
            );
        }
        assert_eq!(super::codec::side(3), None);
    }

    fn temp_segment(tag: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "pm-ws-unit-{tag}-{}-{}.seg",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }

    #[test]
    fn shm_file_backed_segment_is_read_by_an_independent_read_only_mapping() {
        let path = temp_segment("roundtrip");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );
        assert!(region.is_writable());
        assert_eq!(region.size_bytes(), layout().region_size());
        let mut writer =
            SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout(), 42, 5))
                .expect("segment formats");
        let handle = writer.install(&market("mapped")).expect("install");

        let opened = Arc::new(SegmentRegion::open_file(&path).expect("read-only mapping"));
        assert!(!opened.is_writable());
        assert_eq!(
            SegmentWriter::create(Arc::clone(&opened), SegmentConfig::new(layout(), 42, 5),).err(),
            Some(WriterError::RegionNotWritable)
        );
        let reader = SegmentReader::attach(opened).expect("attach");
        assert_eq!(reader.geometry().daemon_instance_id(), 42);
        assert_eq!(reader.geometry().segment_generation(), 5);
        assert_eq!(
            reader
                .resolve(&market("mapped"))
                .map(MarketHandle::entry_index),
            Some(handle.entry_index())
        );
        assert_eq!(reader.read(handle), Err(ReadFault::ForeignSegment));
        let observed = reader.resolve(&market("mapped")).expect("resolve");
        assert_eq!(reader.read(observed), Err(ReadFault::NoPublishedState));

        let mut book = crate::OrderBook::new(market("mapped"));
        assert!(
            book.report_continuity_loss(
                crate::ContinuityReason::Reconnect,
                crate::AuthorityReason::Disconnect
            )
            .expect("loss")
        );
        let published = book.publish();
        writer.publish(handle, &published, 0).expect("publish");
        let snapshot = reader.read(observed).expect("snapshot");
        assert_eq!(snapshot.revision(), published.revision());
        assert_eq!(snapshot.market(), published.market());
        assert!(snapshot.commit_time_nanos().is_some_and(|stamp| stamp > 0));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn shm_create_file_refuses_an_existing_path_and_a_bad_size() {
        let path = temp_segment("exclusive");
        let _first = SegmentRegion::create_file(&path, layout().region_size()).expect("first");
        assert_eq!(
            SegmentRegion::create_file(&path, layout().region_size()).err(),
            Some(RegionError::Io(std::io::ErrorKind::AlreadyExists))
        );
        let unaligned = temp_segment("unaligned");
        assert_eq!(
            SegmentRegion::create_file(&unaligned, 100).err(),
            Some(RegionError::SizeNotBlockAligned)
        );
        assert!(!unaligned.exists());
        let _ = std::fs::remove_file(&path);
    }

    /// Every synchronizing, gate and mutable cell of the v5 layout as
    /// `(name, offset, width)`, grouped by record.
    ///
    /// The width is 0 for the markers that name a size rather than a field. The retained
    /// event slot appears twice, once per delivery kind, because the two kinds overlay one
    /// coordinate space: a mutation's side word and a resolution's winning index are the
    /// same four bytes, and each kind is checked for alignment and overlap on its own. This
    /// is the table `examples/reader.py --abi-table` prints from its own constants, so any
    /// drift between the two implementations fails here instead of silently decoding the
    /// wrong bytes in a consumer.
    fn slot_layout_table() -> Vec<(&'static str, usize, usize)> {
        use super::layout::*;
        vec![
            ("header_magic", HDR_MAGIC, 8),
            (
                "header_publication_generation",
                HDR_PUBLICATION_GENERATION,
                8,
            ),
            ("header_feature_bits", HDR_FEATURE_BITS, 8),
            ("header_event_offset", HDR_EVENT_OFFSET, 8),
            ("header_event_capacity", HDR_EVENT_CAPACITY, 4),
            ("header_event_stride", HDR_EVENT_STRIDE, 4),
            ("header_doorbell", HDR_DOORBELL, 4),
            ("header_dirty_offset", HDR_DIRTY_OFFSET, 8),
            ("header_dirty_capacity", HDR_DIRTY_CAPACITY, 4),
            ("header_dirty_stride", HDR_DIRTY_STRIDE, 4),
            ("header_bytes", HEADER_BYTES, 0),
            ("trailer_region_size", TRL_REGION_SIZE, 8),
            ("trailer_magic", TRL_MAGIC, 8),
            ("trailer_bytes", TRAILER_BYTES, 0),
            ("entry_revision", ENT_REVISION, 8),
            ("entry_state_slot_index", ENT_SLOT_INDEX, 4),
            ("entry_identity_length", ENT_IDENTITY_LEN, 4),
            ("entry_identity", ENT_IDENTITY, IDENTITY_CAPACITY),
            ("directory_entry_bytes", DIRECTORY_ENTRY_BYTES, 0),
            ("slot_revision", SLOT_REVISION, 8),
            ("book_revision", SLOT_BOOK_REVISION, 8),
            ("continuity_epoch", SLOT_CONTINUITY_EPOCH, 8),
            ("continuity_next_position", SLOT_CONTINUITY_POSITION, 8),
            ("sync_divergences", SLOT_SYNC_DIVERGENCES, 8),
            ("authority_state", SLOT_AUTHORITY_STATE, 4),
            ("authority_reason", SLOT_AUTHORITY_REASON, 4),
            ("continuity_kind", SLOT_CONTINUITY_KIND, 4),
            ("continuity_reason", SLOT_CONTINUITY_REASON, 4),
            ("provenance_present", SLOT_PROVENANCE_PRESENT, 4),
            ("provenance_origin", SLOT_ORIGIN, 4),
            ("provenance_derivation", SLOT_DERIVATION, 4),
            ("provenance_representation", SLOT_REPRESENTATION, 4),
            ("native_family_length", SLOT_FAMILY_LEN, 4),
            ("level_count", SLOT_LEVEL_COUNT, 4),
            ("directory_index", SLOT_DIRECTORY_INDEX, 4),
            ("commit_time", SLOT_COMMIT_TIME, 8),
            ("arrival_time", SLOT_ARRIVAL_TIME, 8),
            (
                "native_family_words",
                SLOT_FAMILY_WORDS,
                NATIVE_FAMILY_CAPACITY,
            ),
            ("slot_prefix_bytes", SLOT_PREFIX_BYTES, 0),
            ("level_price", LVL_PRICE, DECIMAL_CELL_BYTES),
            ("level_quantity", LVL_QUANTITY, DECIMAL_CELL_BYTES),
            ("level_side", LVL_SIDE, 4),
            ("level_cell_bytes", LEVEL_CELL_BYTES, 0),
            ("decimal_coefficient_low", DEC_COEFFICIENT_LOW, 8),
            ("decimal_coefficient_high", DEC_COEFFICIENT_HIGH, 8),
            ("decimal_scale", DEC_SCALE, 4),
            ("decimal_cell_bytes", DECIMAL_CELL_BYTES, 0),
            ("event_sequence", EVT_SEQUENCE, 8),
            ("event_cursor_epoch", EVT_CURSOR_EPOCH, 8),
            ("event_cursor_position", EVT_CURSOR_POSITION, 8),
            ("event_book_revision", EVT_BOOK_REVISION, 8),
            ("event_commit_time", EVT_COMMIT_TIME, 8),
            ("event_delivery_kind", EVT_DELIVERY_KIND, 4),
            ("event_origin", EVT_ORIGIN, 4),
            ("event_derivation", EVT_DERIVATION, 4),
            ("event_representation", EVT_REPRESENTATION, 4),
            ("event_side", EVT_SIDE, 4),
            ("event_old_present", EVT_OLD_PRESENT, 4),
            ("event_new_present", EVT_NEW_PRESENT, 4),
            ("event_native_family_length", EVT_FAMILY_LEN, 4),
            ("event_directory_index", EVT_DIRECTORY_INDEX, 4),
            ("event_daemon_generation", EVT_DAEMON_GENERATION, 8),
            (
                "event_subscription_generation",
                EVT_SUBSCRIPTION_GENERATION,
                8,
            ),
            ("event_price", EVT_PRICE, DECIMAL_CELL_BYTES),
            ("event_old_quantity", EVT_OLD_QUANTITY, DECIMAL_CELL_BYTES),
            ("event_new_quantity", EVT_NEW_QUANTITY, DECIMAL_CELL_BYTES),
            (
                "event_native_family_words",
                EVT_FAMILY_WORDS,
                NATIVE_FAMILY_CAPACITY,
            ),
            ("event_arrival_time", EVT_ARRIVAL_TIME, 8),
            ("event_slot_bytes", EVENT_SLOT_BYTES, 0),
            ("resolution_sequence", EVT_SEQUENCE, 8),
            ("resolution_cursor_epoch", EVT_CURSOR_EPOCH, 8),
            ("resolution_cursor_position", EVT_CURSOR_POSITION, 8),
            ("resolution_book_revision", EVT_BOOK_REVISION, 8),
            ("resolution_commit_time", EVT_COMMIT_TIME, 8),
            ("resolution_delivery_kind", EVT_DELIVERY_KIND, 4),
            ("resolution_origin", EVT_ORIGIN, 4),
            ("resolution_derivation", EVT_DERIVATION, 4),
            ("resolution_representation", EVT_REPRESENTATION, 4),
            ("resolution_winning_index", EVT_RES_WINNING_INDEX, 4),
            ("resolution_delivery_path", EVT_RES_DELIVERY_PATH, 4),
            ("resolution_outcome_length", EVT_RES_OUTCOME_LEN, 4),
            ("resolution_type_length", EVT_RES_TYPE_LEN, 4),
            ("resolution_directory_index", EVT_DIRECTORY_INDEX, 4),
            ("resolution_daemon_generation", EVT_DAEMON_GENERATION, 8),
            (
                "resolution_subscription_generation",
                EVT_SUBSCRIPTION_GENERATION,
                8,
            ),
            ("resolution_date_length", EVT_RES_DATE_LEN, 4),
            (
                "resolution_winning_outcome",
                EVT_RES_OUTCOME,
                RES_OUTCOME_CAPACITY,
            ),
            ("resolution_market_type", EVT_RES_TYPE, RES_TYPE_CAPACITY),
            ("resolution_date", EVT_RES_DATE, RES_DATE_CAPACITY),
            ("resolution_arrival_time", EVT_ARRIVAL_TIME, 8),
            ("resolution_slot_bytes", EVENT_SLOT_BYTES, 0),
            ("dirty_sequence", DIRTY_SEQUENCE, 8),
            ("dirty_position", DIRTY_POSITION, 8),
            ("dirty_directory_index", DIRTY_DIRECTORY_INDEX, 4),
            ("dirty_book_revision", DIRTY_BOOK_REVISION, 8),
            ("dirty_slot_bytes", DIRTY_SLOT_BYTES, 0),
        ]
    }

    /// Every field is naturally aligned and no two fields of one record share a byte.
    ///
    /// The table spans several coordinate spaces — the header, the slot prefix, one level
    /// cell, one decimal cell, and one event slot per delivery kind — separated by the
    /// zero-width markers that name each record's size, so occupancy is checked per record
    /// rather than across the whole table.
    #[test]
    fn shm_slot_fields_are_aligned_and_do_not_overlap() {
        assert_eq!(ABI_VERSION, 5);
        let table = slot_layout_table();
        let mut record: Vec<(&str, usize, usize)> = Vec::new();
        for (name, offset, width) in &table {
            if *width == 0 {
                let mut occupied = vec![false; *offset];
                for (field, at, bytes) in &record {
                    assert!(
                        at.is_multiple_of((*bytes).min(8)),
                        "{field} at {at} is not naturally aligned"
                    );
                    for (byte, taken) in occupied.iter_mut().enumerate().skip(*at).take(*bytes) {
                        assert!(
                            !*taken,
                            "{field} overlaps another field of {name} at byte {byte}"
                        );
                        *taken = true;
                    }
                }
                record.clear();
                continue;
            }
            record.push((*name, *offset, *width));
        }
        assert_eq!(
            table
                .iter()
                .find(|(name, ..)| *name == "commit_time")
                .map(|(_, offset, width)| (*offset, *width)),
            Some((88, 8))
        );
    }

    /// Every 64-bit cell carries its high word.
    ///
    /// A 64-bit accessor narrowed to 32 bits would still pass every test that only uses
    /// small counters, and the shared table would still agree because a table pins the
    /// declared width, not the code path. This exercises the code path.
    #[test]
    fn shm_sixty_four_bit_cells_round_trip_their_high_word() {
        use super::layout::TRAILER_BYTES;
        let region = SegmentRegion::zeroed(TRAILER_BYTES).expect("region allocates");
        let record = region
            .cells()
            .record(0, TRAILER_BYTES)
            .expect("record fits the region");
        for value in [
            1_u64 << 32,
            (1_u64 << 32) + 7,
            1_u64 << 63,
            u64::MAX,
            0xdead_beef_feed_face,
        ] {
            record.word64(0).relaxed_store(value);
            record.sync(8).release_store(value);
            assert_eq!(record.word64(0).relaxed_load(), value);
            assert_eq!(record.sync(8).acquire_load(), value);
            record.word32(16).relaxed_store(u32::MAX);
            record.word64(24).relaxed_store(value);
            assert_eq!(record.word32(16).relaxed_load(), u32::MAX);
            assert_eq!(record.word64(24).relaxed_load(), value);
            assert_eq!(record.word32(20).relaxed_load(), 0, "a u64 store spilled");
        }
        let seq = record.seq(32);
        let interval = seq.open_write((1_u64 << 40) + 1);
        seq.close_write(interval, (1_u64 << 40) + 2);
        match seq.open_read() {
            super::cell::SeqOpen::Stable(interval) => assert!(seq.close_read(interval)),
            _ => panic!("a high-word sequence value did not read back as stable"),
        }
    }

    #[test]
    fn shm_python_reader_survives_the_same_decimal_and_discriminant_edges() {
        let output = std::process::Command::new("python3")
            .args(["examples/reader.py", "--self-test"])
            .output()
            .expect("python3 runs examples/reader.py");
        assert!(
            output.status.success(),
            "reader.py --self-test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn shm_python_reader_mirrors_the_v5_slot_layout() {
        let output = std::process::Command::new("python3")
            .args(["examples/reader.py", "--abi-table"])
            .output()
            .expect("python3 runs examples/reader.py");
        assert!(
            output.status.success(),
            "reader.py --abi-table failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mirrored = String::from_utf8(output.stdout).expect("utf-8 table");
        let expected: String = slot_layout_table()
            .iter()
            .map(|(name, offset, width)| format!("{name}={offset}:{width}\n"))
            .collect();
        assert_eq!(mirrored, expected, "the Python reader's layout has drifted");
    }

    /// The claim is exactly-once, and a loser writes nothing.
    ///
    /// This is the deterministic half of the concurrent-formatting guarantee: whatever the
    /// interleaving, `publish_write_once` succeeds for one caller and refuses every other,
    /// and a refused caller leaves the payload exactly as the winner wrote it. The threaded
    /// test below samples real interleavings, but only this one is a proof.
    #[test]
    fn shm_a_write_once_gate_is_claimed_exactly_once_and_a_loser_writes_nothing() {
        use super::cell::{GATE_CLAIMED, WriteOnceField};
        use super::layout::{TRAILER_BYTES, TRL_MAGIC, TRL_REGION_SIZE};
        let region = SegmentRegion::zeroed(layout().region_size()).expect("region allocates");
        let trailer = super::layout::trailer_record(region.cells()).expect("trailer record");
        assert!(trailer.published(TRL_MAGIC).is_none());
        assert!(trailer.publish_write_once(
            TRL_MAGIC,
            super::layout::TRAILER_MAGIC,
            &[WriteOnceField::Word64 {
                offset: TRL_REGION_SIZE,
                value: 0x1111_1111,
            }],
        ));
        assert!(!trailer.publish_write_once(
            TRL_MAGIC,
            super::layout::TRAILER_MAGIC,
            &[WriteOnceField::Word64 {
                offset: TRL_REGION_SIZE,
                value: 0x2222_2222,
            }],
        ));
        let witness = trailer.published(TRL_MAGIC).expect("published trailer");
        assert_eq!(witness.word64(TRL_REGION_SIZE), 0x1111_1111);

        let claimed = SegmentRegion::zeroed(TRAILER_BYTES).expect("region allocates");
        let record = claimed
            .cells()
            .record(0, TRAILER_BYTES)
            .expect("claimed record");
        record.sync(TRL_MAGIC).release_store(GATE_CLAIMED);
        assert!(
            record.published(TRL_MAGIC).is_none(),
            "a claimed gate must never yield a witness"
        );
        assert!(
            !record.publish_write_once(
                TRL_MAGIC,
                super::layout::TRAILER_MAGIC,
                &[WriteOnceField::Word64 {
                    offset: TRL_REGION_SIZE,
                    value: 0x3333_3333,
                }],
            ),
            "a claimed gate must refuse a second formatter"
        );
        assert_eq!(record.word64(TRL_REGION_SIZE).relaxed_load(), 0);
    }

    /// Samples real interleavings of two formatters on one region.
    ///
    /// A regression net rather than a proof: it detects a racy claim only on the schedules
    /// it happens to hit, and on this host a read-then-write claim survives it. The proof is
    /// the exactly-once test above plus the compare-exchange itself.
    #[test]
    fn shm_concurrent_formatters_never_both_write_the_header() {
        for round in 0..2_000_u64 {
            let region =
                Arc::new(SegmentRegion::zeroed(layout().region_size()).expect("region allocates"));
            let gate = std::sync::atomic::AtomicU32::new(0);
            let outcomes: Vec<Result<SegmentWriter, WriterError>> = std::thread::scope(|scope| {
                let threads: Vec<_> = (0..2_u128)
                    .map(|thread| {
                        let region = Arc::clone(&region);
                        let gate = &gate;
                        scope.spawn(move || {
                            let _ = gate.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            while gate.load(std::sync::atomic::Ordering::Acquire) < 2 {
                                std::hint::spin_loop();
                            }
                            SegmentWriter::create(
                                region,
                                SegmentConfig::new(layout(), u128::from(round) << 8 | thread, 1),
                            )
                        })
                    })
                    .collect();
                threads
                    .into_iter()
                    .map(|thread| thread.join().expect("formatter thread"))
                    .collect()
            });
            let formatted = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
            assert_eq!(formatted, 1, "round {round}: {formatted} formatters won");
            for outcome in &outcomes {
                if let Err(error) = outcome {
                    assert_eq!(*error, WriterError::SegmentAlreadyFormatted);
                }
            }
            assert!(
                validate(&region).is_ok(),
                "round {round} left an invalid header"
            );
        }
    }

    #[test]
    fn shm_validator_rejects_a_previous_abi_version() {
        let (region, _writer) = formatted();
        let header = super::layout::header_record(region.cells()).expect("header record");
        header
            .word32(super::layout::HDR_ABI_VERSION)
            .relaxed_store(1);
        assert_eq!(
            validate(&region),
            Err(SegmentFault::AbiVersionUnsupported {
                found: 1,
                expected: ABI_VERSION
            })
        );
    }

    #[test]
    fn shm_validator_rejects_every_individually_corrupted_geometry_field() {
        use super::layout::{
            HDR_DIRECTORY_CAPACITY, HDR_DIRECTORY_OFFSET, HDR_DIRECTORY_STRIDE, HDR_DIRTY_CAPACITY,
            HDR_DIRTY_OFFSET, HDR_DIRTY_STRIDE, HDR_EVENT_CAPACITY, HDR_EVENT_OFFSET,
            HDR_EVENT_STRIDE, HDR_FAMILY_CAPACITY, HDR_FEATURE_BITS, HDR_IDENTITY_CAPACITY,
            HDR_LEVEL_CAPACITY, HDR_LEVEL_STRIDE, HDR_REGION_ALIGNMENT, HDR_REGION_SIZE,
            HDR_SLOT_CAPACITY, HDR_SLOT_OFFSET, HDR_SLOT_STRIDE, HDR_TRAILER_OFFSET, TRL_MAGIC,
            TRL_REGION_SIZE,
        };
        for offset in [
            HDR_REGION_ALIGNMENT,
            HDR_DIRECTORY_STRIDE,
            HDR_DIRECTORY_CAPACITY,
            HDR_SLOT_STRIDE,
            HDR_SLOT_CAPACITY,
            HDR_LEVEL_CAPACITY,
            HDR_LEVEL_STRIDE,
            HDR_IDENTITY_CAPACITY,
            HDR_FAMILY_CAPACITY,
            HDR_EVENT_CAPACITY,
            HDR_EVENT_STRIDE,
            HDR_DIRTY_CAPACITY,
            HDR_DIRTY_STRIDE,
        ] {
            let (region, _writer) = formatted();
            let header = super::layout::header_record(region.cells()).expect("header record");
            let corrupted = header.word32(offset).relaxed_load().wrapping_add(1);
            header.word32(offset).relaxed_store(corrupted);
            assert!(
                validate(&region).is_err(),
                "corrupting the 32-bit field at {offset} still validated"
            );
        }
        for offset in [
            HDR_REGION_SIZE,
            HDR_DIRECTORY_OFFSET,
            HDR_SLOT_OFFSET,
            HDR_EVENT_OFFSET,
            HDR_DIRTY_OFFSET,
            HDR_TRAILER_OFFSET,
            HDR_FEATURE_BITS,
        ] {
            let (region, _writer) = formatted();
            let header = super::layout::header_record(region.cells()).expect("header record");
            let corrupted = header.word64(offset).relaxed_load().wrapping_add(8);
            header.word64(offset).relaxed_store(corrupted);
            assert!(
                validate(&region).is_err(),
                "corrupting the 64-bit field at {offset} still validated"
            );
        }
        for offset in [TRL_REGION_SIZE, TRL_MAGIC] {
            let (region, _writer) = formatted();
            let trailer = super::layout::trailer_record(region.cells()).expect("trailer record");
            let corrupted = trailer.word64(offset).relaxed_load().wrapping_add(8);
            trailer.word64(offset).relaxed_store(corrupted);
            assert_eq!(validate(&region), Err(SegmentFault::TrailerMismatch));
        }
    }

    #[test]
    fn shm_reader_refuses_a_slot_index_outside_the_segment() {
        let (region, _writer) = formatted();
        let outside = layout().state_slot_capacity();
        let entry =
            super::layout::directory_record(region.cells(), layout(), 2).expect("entry record");
        entry.word32(ENT_SLOT_INDEX).relaxed_store(outside);
        entry.sync(ENT_REVISION).release_store(1);
        let reader = SegmentReader::attach(Arc::clone(&region)).expect("attach");
        assert_eq!(
            reader.read(MarketHandle::new(reader.binding(), 2, outside)),
            Err(ReadFault::MalformedRecord)
        );
    }

    #[test]
    fn shm_handles_are_bound_to_their_segment_and_their_market() {
        let (_region_a, mut writer_a) = formatted();
        let (region_b, mut writer_b) = formatted();
        let handle_a = writer_a.install(&market("alpha")).expect("install alpha");
        let handle_b = writer_b.install(&market("alpha")).expect("install alpha");
        let other_b = writer_b.install(&market("beta")).expect("install beta");
        assert_ne!(handle_a.segment(), handle_b.segment());

        let mut alpha = crate::OrderBook::new(market("alpha"));
        assert!(
            alpha
                .report_continuity_loss(
                    crate::ContinuityReason::Reconnect,
                    crate::AuthorityReason::Disconnect
                )
                .expect("loss")
        );
        let published = alpha.publish();

        assert_eq!(
            writer_b.publish(handle_a, &published, 0).err(),
            Some(WriterError::UnknownHandle)
        );
        assert_eq!(
            writer_b.publish(other_b, &published, 0).err(),
            Some(WriterError::MarketMismatch)
        );
        assert!(writer_b.publish(handle_b, &published, 0).is_ok());

        let reader_b = SegmentReader::attach(Arc::clone(&region_b)).expect("attach");
        assert_eq!(reader_b.read(handle_a), Err(ReadFault::ForeignSegment));
        assert!(reader_b.read(handle_b).is_ok());
    }

    /// The decimal cases both decoders must agree on, exactly.
    ///
    /// The same accept and reject lists are in `examples/reader.py --self-test`. They
    /// include the two canonicalization cases — a non-canonical zero and a trailing-zero
    /// coefficient — and the sign bit, which a price or a quantity may never carry.
    #[test]
    fn shm_decimals_survive_the_edges_and_refuse_a_hostile_scale() {
        use crate::{DecimalGrammar, Price, Quantity};
        let grammar = DecimalGrammar::new(u16::MAX, 39, false, false).expect("grammar");
        let widest = i128::MAX.unsigned_abs();
        let (low, high) = (widest as u64, (widest >> 64) as u64);
        let accepted: [((u64, u64, u32), &str); 6] = [
            ((0, 0, 0), "0"),
            ((0, 0, 5), "0"),
            ((1200, 0, 2), "12"),
            ((1, 0, u32::from(u16::MAX)), ""),
            ((low, high, 0), "170141183460469231731687303715884105727"),
            ((low, high, 38), "1.70141183460469231731687303715884105727"),
        ];
        for ((low, high, scale), expected) in accepted {
            let price = super::codec::price(low, high, scale)
                .unwrap_or_else(|| panic!("price ({low}, {high}, {scale}) did not decode"));
            if !expected.is_empty() {
                assert_eq!(price.value().canonical(), expected);
            } else {
                assert_eq!(price.value().scale(), u16::MAX);
            }
            let (again_low, again_high, again_scale) = super::codec::decimal_words(price.value());
            assert_eq!(
                super::codec::price(again_low, again_high, again_scale),
                Some(price)
            );
            assert!(super::codec::quantity(low, high, scale).is_some());
        }
        let rejected: [(u64, u64, u32); 4] = [
            (0, 1 << 63, 0),
            (low, high | (1 << 63), 0),
            (1, 0, u32::from(u16::MAX) + 1),
            (1, 0, u32::MAX),
        ];
        for (low, high, scale) in rejected {
            assert_eq!(super::codec::price(low, high, scale), None);
            assert_eq!(super::codec::quantity(low, high, scale), None);
        }
        assert_eq!(
            Quantity::from_parts(1, 0, u16::MAX, grammar).map(|value| value.value().scale()),
            Ok(u16::MAX)
        );
        assert!(Price::from_parts(0, 1 << 63, 0, grammar).is_err());
    }

    #[test]
    fn shm_layout_refuses_fewer_state_slots_than_directory_entries() {
        assert_eq!(
            SegmentLayout::new(4, 3, 8, 8, 16).err(),
            Some(LayoutError::StateSlotsBelowDirectory)
        );
        assert!(SegmentLayout::new(4, 5, 8, 8, 16).is_ok());
    }

    /// A ring depth that cannot be masked is refused, so a position never needs a division.
    #[test]
    fn shm_layout_refuses_an_event_capacity_that_is_not_a_power_of_two() {
        for capacity in [3, 6, 100, 1023] {
            assert_eq!(
                SegmentLayout::new(1, 1, 8, capacity, 16).err(),
                Some(LayoutError::EventCapacityNotPowerOfTwo),
                "event capacity {capacity} was accepted"
            );
        }
        assert_eq!(
            SegmentLayout::new(1, 1, 8, 0, 16).err(),
            Some(LayoutError::CapacityZero)
        );
        for capacity in [1, 2, 1_024, 65_536] {
            assert!(SegmentLayout::new(1, 1, 8, capacity, 16).is_ok());
        }
    }

    /// The feature word fails closed every way: an unknown bit, a missing required one, and a
    /// doorbell placement that is not exactly one of the two.
    ///
    /// Neither doorbell bit leaves a consumer nowhere to park; both leave it parked on an
    /// address the writer may not be ringing, which is a wake-up loss no consumer could
    /// diagnose. Only exactly one is a segment this build will read.
    #[test]
    fn shm_validator_refuses_a_feature_word_it_cannot_honour() {
        for bits in [
            0,
            FEATURE_EVENT_RING_WRAPS | (1 << 3),
            1 << 7,
            u64::MAX,
            FEATURE_EVENT_RING_WRAPS,
            FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE,
            FEATURE_EVENT_RING_WRAPS | FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE,
        ] {
            let (region, _writer) = formatted();
            let header = super::layout::header_record(region.cells()).expect("header record");
            header
                .word64(super::layout::HDR_FEATURE_BITS)
                .relaxed_store(bits);
            assert_eq!(
                validate(&region),
                Err(SegmentFault::FeatureUnsupported { bits })
            );
        }
    }

    /// One published mutation, ready to have exactly one of its words corrupted.
    ///
    /// The book is fresh, so its published state is revision 0 with an intact stream at
    /// epoch 0 position 0 — the coordinates the mutation below is published at.
    fn mutating_segment() -> (Arc<SegmentRegion>, SegmentWriter, MarketHandle) {
        mutating_segment_stamped(0)
    }

    /// [`mutating_segment`] stamping `arrival` on every publication it makes.
    fn mutating_segment_stamped(arrival: u64) -> (Arc<SegmentRegion>, SegmentWriter, MarketHandle) {
        let (region, mut writer) = formatted();
        let market = market("mutating");
        let handle = writer.install(&market).expect("install");
        let book = crate::OrderBook::new(market.clone());
        writer
            .publish(handle, &book.publish(), arrival)
            .expect("publish");
        let grammar = crate::DecimalGrammar::new(18, 30, true, false).expect("grammar");
        let level = |quantity: &str| {
            crate::Level::new(
                crate::Side::Bid,
                crate::Price::parse("0.500", grammar).expect("price"),
                crate::Quantity::parse(quantity, grammar).expect("quantity"),
            )
        };
        let provenance = crate::Provenance::new(crate::ProvenanceInput {
            market,
            outcome: None,
            native_family: "orderbookUpdate".into(),
            source_timestamp: None,
            source_evidence: crate::BoundedSourceEvidence::new(
                [],
                crate::SourceEvidenceCapacity::new(0).expect("capacity"),
            )
            .expect("evidence"),
            daemon_generation: 3,
            connection: crate::ConnectionIdentity::new("ws.limitless.exchange", 1)
                .expect("connection"),
            subscription_generation: 5,
            receive_position: 0,
            commit_position: 0,
            local_receive_time: crate::LocalMonotonicTimestamp::new(0),
            local_commit_time: crate::LocalMonotonicTimestamp::new(0),
            replica: crate::ReplicaRole::PublishingPrimary,
            representation: crate::Representation::VenueNative,
            origin: crate::Origin::SourceReported,
            local_revision: 1,
            continuity_epoch: 0,
        })
        .expect("provenance");
        let mutation =
            crate::BookMutation::source_reported(provenance, Some(level("10")), Some(level("20")))
                .expect("mutation");
        writer
            .publish_mutation(
                handle,
                1,
                &crate::MutationCursor::new(0, 0),
                &mutation,
                arrival,
            )
            .expect("publish mutation");
        (region, writer, handle)
    }

    /// An event slot serving another entry, or carrying a word this ABI does not define, is
    /// refused rather than decoded into a guess.
    ///
    /// Delivery kind 2 is the FFI's synthesized continuity-loss marker: the writer never
    /// stores it, so a slot holding it is malformed and never a loss.
    #[test]
    fn shm_event_slot_ownership_and_discriminants_are_refused_rather_than_guessed() {
        use super::layout::{
            EVT_DELIVERY_KIND, EVT_DIRECTORY_INDEX, EVT_FAMILY_LEN, EVT_NEW_PRESENT,
            EVT_OLD_PRESENT, EVT_ORIGIN, EVT_REPRESENTATION, EVT_SIDE,
        };
        let (region, _writer, handle) = mutating_segment();
        let reader = SegmentReader::attach(Arc::clone(&region)).expect("attach");
        let (_, mut stream) = reader.attach_stream(handle).expect("attach stream");
        assert!(matches!(stream.poll(), Ok(EventPoll::Delivered(_))));

        let corruptions: [(&[(usize, u32)], ReadFault); 9] = [
            (
                &[(EVT_DIRECTORY_INDEX, 1)],
                ReadFault::SlotOwnershipMismatch,
            ),
            (&[(EVT_DELIVERY_KIND, 2)], ReadFault::MalformedRecord),
            (&[(EVT_DELIVERY_KIND, 0)], ReadFault::MalformedRecord),
            (&[(EVT_SIDE, 3)], ReadFault::MalformedRecord),
            (&[(EVT_ORIGIN, 9)], ReadFault::MalformedRecord),
            (&[(EVT_REPRESENTATION, 0)], ReadFault::MalformedRecord),
            (&[(EVT_OLD_PRESENT, 2)], ReadFault::MalformedRecord),
            (
                &[(EVT_OLD_PRESENT, 0), (EVT_NEW_PRESENT, 0)],
                ReadFault::MalformedRecord,
            ),
            (
                &[(EVT_FAMILY_LEN, NATIVE_FAMILY_CAPACITY as u32 + 1)],
                ReadFault::MalformedRecord,
            ),
        ];
        for (stores, expected) in corruptions {
            let (region, _writer, handle) = mutating_segment();
            let slot = super::layout::event_slot_record(region.cells(), layout(), 0, 0)
                .expect("event slot");
            for (offset, value) in stores {
                slot.word32(*offset).relaxed_store(*value);
            }
            let reader = SegmentReader::attach(Arc::clone(&region)).expect("attach");
            let (_, mut stream) = reader.attach_stream(handle).expect("attach stream");
            let refused = Err(StreamFault::Read(expected));
            assert_eq!(
                stream.poll(),
                refused,
                "corrupting {stores:?} was decoded rather than refused"
            );
            assert_eq!(
                stream.poll(),
                refused,
                "a malformed slot must keep answering the same way, never advance"
            );
            assert_eq!(stream.cursor().position(), 0);
        }
    }

    /// The state slot's arrival stamp round-trips, and 0 means "no venue frame drove this".
    #[test]
    fn shm_state_arrival_stamps_round_trip_and_read_zero_when_absent() {
        for arrival in [0_u64, 1, 1_756_745_000_123_456_789, u64::MAX] {
            let (region, mut writer) = formatted();
            let handle = writer.install(&market("stamped")).expect("install");
            let book = crate::OrderBook::new(market("stamped"));
            writer
                .publish(handle, &book.publish(), arrival)
                .expect("publish");
            let slot =
                super::layout::state_slot_record(region.cells(), layout(), 0).expect("slot record");
            assert_eq!(slot.word64(SLOT_ARRIVAL_TIME).relaxed_load(), arrival);
        }
    }

    /// A republication that commits no revision carries both stamps forward byte for byte.
    ///
    /// Restamping either one would make a book that has not moved look freshly committed, or
    /// freshly arrived, to a consumer keying freshness on them.
    #[test]
    fn shm_a_republication_carries_both_stamps_and_never_restamps_them() {
        let (region, mut writer) = formatted();
        let handle = writer.install(&market("carried")).expect("install");
        let book = crate::OrderBook::new(market("carried"));
        let arrival = 1_756_745_000_123_456_789_u64;
        writer
            .publish(handle, &book.publish(), arrival)
            .expect("publish");
        let slot =
            super::layout::state_slot_record(region.cells(), layout(), 0).expect("slot record");
        let commit = slot.word64(super::layout::SLOT_COMMIT_TIME).relaxed_load();
        assert!(commit > 0);

        writer
            .republish_carrying_stamps(handle, &book.publish())
            .expect("republish");
        assert_eq!(slot.word64(SLOT_ARRIVAL_TIME).relaxed_load(), arrival);
        assert_eq!(
            slot.word64(super::layout::SLOT_COMMIT_TIME).relaxed_load(),
            commit
        );
    }

    /// An event slot's arrival stamp round-trips under both delivery kinds.
    ///
    /// The cell is the same eight bytes for a mutation and for a resolution, which is why the
    /// two overlays can share it; a stamp written under one kind and read under the other
    /// would be the drift this catches.
    #[test]
    fn shm_event_arrival_stamps_round_trip_under_both_delivery_kinds() {
        let arrival = 1_756_745_111_222_333_444_u64;
        let (region, _writer, _handle) = mutating_segment_stamped(arrival);
        let slot =
            super::layout::event_slot_record(region.cells(), layout(), 0, 0).expect("event slot");
        assert_eq!(slot.word64(EVT_ARRIVAL_TIME).relaxed_load(), arrival);

        let (region, _writer, _handle) = mutating_segment_stamped(0);
        let slot =
            super::layout::event_slot_record(region.cells(), layout(), 0, 0).expect("event slot");
        assert_eq!(slot.word64(EVT_ARRIVAL_TIME).relaxed_load(), 0);
    }

    /// Every state publication appends exactly one dirty entry, and the ring wraps by mask.
    ///
    /// The position is absolute and segment-lifetime monotone, so a consumer whose expectation
    /// is overtaken reads a *greater* position in its slot — the declared full-rescan signal —
    /// rather than a silently reused one.
    #[test]
    fn shm_every_state_publication_appends_one_dirty_entry() {
        let (region, mut writer) = formatted();
        let first = writer.install(&market("alpha")).expect("install alpha");
        let second = writer.install(&market("beta")).expect("install beta");
        let alpha = crate::OrderBook::new(market("alpha"));
        let beta = crate::OrderBook::new(market("beta"));

        let capacity = u64::from(layout().dirty_capacity());
        let rounds = capacity + 3;
        for round in 0..rounds {
            let (handle, published) = if round % 2 == 0 {
                (first, alpha.publish())
            } else {
                (second, beta.publish())
            };
            writer.publish(handle, &published, 0).expect("publish");

            let slot = super::layout::dirty_slot_record(region.cells(), layout(), round)
                .expect("dirty slot");
            assert_eq!(slot.word64(DIRTY_POSITION).relaxed_load(), round);
            assert_eq!(
                slot.word32(DIRTY_DIRECTORY_INDEX).relaxed_load(),
                handle.entry_index()
            );
            assert_eq!(
                slot.word64(DIRTY_BOOK_REVISION).relaxed_load(),
                published.revision()
            );
            assert_eq!(
                slot.seq(DIRTY_SEQUENCE).writer_current() % 2,
                0,
                "a dirty entry must be left stable, never in flight"
            );
        }

        let wrapped =
            super::layout::dirty_slot_record(region.cells(), layout(), 0).expect("dirty slot");
        assert_eq!(
            wrapped.word64(DIRTY_POSITION).relaxed_load(),
            capacity,
            "the ring wrapped without the position resetting"
        );
    }

    /// The doorbell advances once per publication generation bump, whatever the delivery.
    #[test]
    fn shm_the_doorbell_advances_with_every_generation_bump() {
        let (region, mut writer, handle) = mutating_segment_stamped(0);
        let header = super::layout::header_record(region.cells()).expect("header record");
        let observed = header.word32(HDR_DOORBELL).relaxed_load();
        let generation = writer.publication_generation();
        assert_eq!(u64::from(observed), generation);

        let book = crate::OrderBook::new(market("mutating"));
        writer
            .publish(handle, &book.publish(), 0)
            .expect("republish");
        assert_eq!(
            header.word32(HDR_DOORBELL).relaxed_load(),
            observed.wrapping_add(1)
        );
        assert_eq!(writer.publication_generation(), generation + 1);
    }

    /// One wake syscall per publication round, and none for a mutation.
    ///
    /// A commit of *n* mutations plus its state therefore costs one wake rather than *n* + 1,
    /// which is the whole reason the wake is posted by the state publication alone. With no
    /// waiter the platform answers "nobody was parked", which is not a fault.
    #[test]
    fn shm_one_wake_is_posted_per_publication_round() {
        let (_region, mut writer, handle) = mutating_segment_stamped(0);
        assert_eq!(
            writer.wake_posts(),
            1,
            "the install-time state publication is the only wake so far"
        );
        assert_eq!(writer.wake_faults(), 0);

        let book = crate::OrderBook::new(market("mutating"));
        for round in 1..4_u64 {
            writer
                .publish(handle, &book.publish(), 0)
                .expect("republish");
            assert_eq!(writer.wake_posts(), round + 1);
        }
        assert_eq!(
            writer.wake_faults(),
            0,
            "a wake with no waiters is not a fault"
        );
    }

    /// A heap-backed region keeps its doorbell in the header and refuses a sibling page.
    ///
    /// Its readers share the writer's own writable mapping, so the header word is exactly as
    /// waitable as any address in this process and there is no file to put a page beside.
    #[test]
    fn shm_a_heap_region_keeps_its_doorbell_in_the_header() {
        let (_region, writer) = formatted();
        assert_eq!(writer.doorbell_feature_bit(), FEATURE_DOORBELL_IN_HEADER);

        let region =
            Arc::new(SegmentRegion::zeroed(layout().region_size()).expect("region allocates"));
        let forced = SegmentWriter::create(
            region,
            SegmentConfig {
                doorbell: DoorbellPlacement::ForcePage,
                ..SegmentConfig::new(layout(), 1, 1)
            },
        );
        assert_eq!(forced.err(), Some(WriterError::DoorbellPageUnavailable));
    }

    /// A file-backed segment declares exactly one doorbell placement, and the probe's answer
    /// is the platform's, not this build's assumption.
    ///
    /// The answer is genuinely the kernel's to give: the same Darwin host has been measured
    /// refusing a read-only wait at one time and accepting it at another, which is why this
    /// test pins only that exactly one bit is set and either placement validates.
    #[test]
    fn shm_a_file_backed_segment_declares_one_probed_doorbell_placement() {
        let path = temp_segment("doorbell-probe");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );
        let writer =
            SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout(), 11, 1))
                .expect("segment formats");
        let bit = writer.doorbell_feature_bit();
        assert!(bit == FEATURE_DOORBELL_IN_HEADER || bit == FEATURE_DOORBELL_PAGE);
        let header = super::layout::header_record(region.cells()).expect("header record");
        assert_eq!(
            header
                .word64(super::layout::HDR_FEATURE_BITS)
                .relaxed_load(),
            FEATURE_EVENT_RING_WRAPS | bit
        );
        assert!(validate(&region).is_ok());
        assert_eq!(
            std::path::PathBuf::from(format!("{}.doorbell", path.display())).exists(),
            bit == FEATURE_DOORBELL_PAGE
        );

        drop(writer);
        let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
        let _ = std::fs::remove_file(&path);
    }

    /// The page fallback is exercised deterministically even where the probe succeeds, and
    /// the page it creates is owner-only.
    ///
    /// The mode is the finding, not a detail: a page any reader-group principal could open
    /// read-write is a page any of them could truncate, and the writer's next mirrored store
    /// into a shortened mapping takes `SIGBUS`. 0600 is what keeps a consumer from being able
    /// to kill ingestion.
    #[test]
    fn shm_forcing_the_page_creates_an_owner_only_sibling() {
        let path = temp_segment("doorbell-forced");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );
        let mut writer = SegmentWriter::create(
            Arc::clone(&region),
            SegmentConfig {
                doorbell: DoorbellPlacement::ForcePage,
                ..SegmentConfig::new(layout(), 12, 1)
            },
        )
        .expect("segment formats");
        assert_eq!(writer.doorbell_feature_bit(), FEATURE_DOORBELL_PAGE);
        let page = std::path::PathBuf::from(format!("{}.doorbell", path.display()));
        assert!(page.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&page)
                .expect("page metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                mode, 0o600,
                "the page must be owner-only: a group-writable page is a truncatable one"
            );
        }

        let handle = writer.install(&market("paged")).expect("install");
        let book = crate::OrderBook::new(market("paged"));
        writer.publish(handle, &book.publish(), 0).expect("publish");
        let header = super::layout::header_record(region.cells()).expect("header record");
        let mirrored = SegmentRegion::open_file(&page).expect("read the page back");
        assert_eq!(
            mirrored
                .cells()
                .record(0, REGION_ALIGNMENT)
                .expect("page record")
                .word32(0)
                .relaxed_load(),
            header.word32(HDR_DOORBELL).relaxed_load(),
            "the page must mirror every doorbell store"
        );

        drop(writer);
        let _ = std::fs::remove_file(&page);
        let _ = std::fs::remove_file(&path);
    }

    /// A path already occupied by something else is refused, and what sits there survives.
    ///
    /// The segment path is an operator argument, so `<segment path>.doorbell` can name any
    /// file, FIFO, or socket an operator's typo reaches. Creation is exclusive and this
    /// writer removes nothing it did not create: the collision is a typed refusal naming the
    /// path, and clearing a page a dead writer left behind is an operator action.
    #[test]
    fn shm_a_doorbell_page_path_that_is_occupied_is_refused_and_left_untouched() {
        const SENTINEL: &[u8] = b"not a doorbell page: an operator's unrelated file\n";
        let path = temp_segment("doorbell-occupied");
        let page = std::path::PathBuf::from(format!("{}.doorbell", path.display()));
        std::fs::write(&page, SENTINEL).expect("write the sentinel");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );

        let refused = SegmentWriter::create(
            Arc::clone(&region),
            SegmentConfig {
                doorbell: DoorbellPlacement::ForcePage,
                ..SegmentConfig::new(layout(), 14, 1)
            },
        );
        assert_eq!(
            refused.err(),
            Some(WriterError::DoorbellPageOccupied(page.clone())),
            "an occupied page path must be a typed refusal naming it"
        );
        assert_eq!(
            std::fs::read(&page).expect("the sentinel survives"),
            SENTINEL,
            "the writer unlinked or rewrote a file it did not create"
        );

        let _ = std::fs::remove_file(&page);
        let _ = std::fs::remove_file(&path);
    }

    /// A parked thread is released by a publication, on the address this segment actually
    /// declares.
    ///
    /// The generous timeout is what keeps a failure a failure rather than a hung suite: the
    /// wait ends either way, and the assertion is about which way. This parks on the writer's
    /// own mapping of the doorbell — a consumer's read-only mapping is the reader wave's — so
    /// what it proves is the wake path itself: bump, syscall, waiter released.
    ///
    /// The assertion is "not a timeout" rather than "woken" because both non-timeout answers
    /// are the property under test: the publication released the waiter either by its wake or,
    /// if the thread had not reached the syscall yet, by the doorbell already differing, which
    /// is the very recheck that closes the lost-wake window. A platform that reports the second
    /// case separately — Linux answers `EAGAIN` — must not turn a scheduling delay into a
    /// failure. [`shm_a_park_with_no_wake_times_out`] pins the negative case deterministically,
    /// so the pair still separates a working wake from everything timing out.
    #[test]
    fn shm_a_parked_thread_is_woken_by_a_publication() {
        let path = temp_segment("doorbell-park");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );
        let mut writer =
            SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout(), 13, 1))
                .expect("segment formats");
        let handle = writer.install(&market("parked")).expect("install");
        let book = crate::OrderBook::new(market("parked"));
        writer
            .publish(handle, &book.publish(), 0)
            .expect("first publish");

        let address = writer.doorbell_address();
        let observed = super::layout::header_record(region.cells())
            .expect("header record")
            .word32(HDR_DOORBELL)
            .relaxed_load();
        let woken = std::thread::scope(|scope| {
            let parked = scope.spawn(move || {
                super::doorbell::wait(address, observed, Some(Duration::from_secs(10)))
            });
            std::thread::sleep(Duration::from_millis(50));
            writer
                .publish(handle, &book.publish(), 0)
                .expect("waking publish");
            parked.join().expect("the parked thread joins")
        });
        assert_ne!(woken, Ok(super::doorbell::WaitOutcome::TimedOut));
        assert!(
            woken.is_ok(),
            "the park failed rather than ending: {woken:?}"
        );

        let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
        let _ = std::fs::remove_file(&path);
    }

    /// The wake side's own report proves a *registered* waiter on a second mapping was
    /// released, not merely that a value changed under a fast path.
    ///
    /// Every other doorbell test asserts "the wait ended, and not by timeout", which a
    /// pre-park value recheck satisfies without any kernel wake ever happening. This one
    /// takes the other end: a thread parks on the address a consumer would use — the sibling
    /// page's own mapping where the segment declared one, the segment's read-only mapping
    /// where it did not, never the writer's — and the wake is reposted until the platform
    /// itself says it found somebody. Linux answers a woken count, Darwin distinguishes
    /// "found the address" from `ENOENT` "nobody there", so
    /// [`doorbell::WakeOutcome::Woken`] is the portable predicate for both.
    ///
    /// The wake loop is what makes this deterministic rather than a race: the parking thread
    /// may not have reached the syscall on the first attempt, so the wake is retried against
    /// a deadline instead of being posted once and hoped over.
    #[test]
    fn shm_a_wake_reports_releasing_a_waiter_registered_on_another_mapping() {
        let path = temp_segment("doorbell-registered");
        let region = Arc::new(
            SegmentRegion::create_file(&path, layout().region_size()).expect("segment file"),
        );
        let writer =
            SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout(), 15, 1))
                .expect("segment formats");
        let page = std::path::PathBuf::from(format!("{}.doorbell", path.display()));

        let consumer = if writer.doorbell_feature_bit() == FEATURE_DOORBELL_PAGE {
            SegmentRegion::open_page_read_write(&page).expect("open the sibling page read-write")
        } else {
            SegmentRegion::open_file(&path).expect("open the segment read-only")
        };
        let consumer_cell = if writer.doorbell_feature_bit() == FEATURE_DOORBELL_PAGE {
            consumer
                .cells()
                .record(0, REGION_ALIGNMENT)
                .expect("page record")
                .word32(0)
        } else {
            super::layout::header_record(consumer.cells())
                .expect("header record")
                .word32(HDR_DOORBELL)
        };
        let address = consumer_cell.wake_address();
        let observed = consumer_cell.relaxed_load();

        let released = std::thread::scope(|scope| {
            let parked =
                scope.spawn(move || super::doorbell::wait(address, observed, Some(WAKE_DEADLINE)));
            let mut reported = None;
            let started = std::time::Instant::now();
            while reported.is_none() && started.elapsed() < WAKE_DEADLINE {
                match super::doorbell::wake_all(writer.doorbell_address()) {
                    Ok(super::doorbell::WakeOutcome::Woken(count)) => reported = Some(count),
                    Ok(super::doorbell::WakeOutcome::NoWaiters) => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(fault) => panic!("the wake itself failed: {fault}"),
                }
            }
            let outcome = parked.join().expect("the parked thread joins");
            (reported, outcome)
        });

        let (reported, outcome) = released;
        assert!(
            reported.is_some(),
            "no wake in {WAKE_DEADLINE:?} ever reported finding a registered waiter"
        );
        assert_eq!(
            outcome,
            Ok(super::doorbell::WaitOutcome::Woken),
            "the released waiter did not report a wake"
        );

        let _ = std::fs::remove_file(&page);
        let _ = std::fs::remove_file(&path);
    }

    /// A wait that nobody wakes ends at its own deadline rather than hanging.
    #[test]
    fn shm_a_park_with_no_wake_times_out() {
        let (region, writer) = formatted();
        let address = writer.doorbell_address();
        let observed = super::layout::header_record(region.cells())
            .expect("header record")
            .word32(HDR_DOORBELL)
            .relaxed_load();
        let started = std::time::Instant::now();
        assert_eq!(
            super::doorbell::wait(address, observed, Some(Duration::from_millis(50))),
            Ok(super::doorbell::WaitOutcome::TimedOut)
        );
        assert!(started.elapsed() >= Duration::from_millis(40));
    }

    /// A dirty depth that cannot be masked is refused, so a position never needs a division.
    #[test]
    fn shm_layout_refuses_a_dirty_capacity_that_is_not_a_power_of_two() {
        for capacity in [3, 6, 100, 1023] {
            assert_eq!(
                SegmentLayout::new(1, 1, 8, 8, capacity).err(),
                Some(LayoutError::DirtyCapacityNotPowerOfTwo),
                "dirty capacity {capacity} was accepted"
            );
        }
        assert_eq!(
            SegmentLayout::new(1, 1, 8, 8, 0).err(),
            Some(LayoutError::CapacityZero)
        );
        assert_eq!(
            SegmentLayout::new(1, 1, 8, 8, 1 << 21).err(),
            Some(LayoutError::CapacityTooLarge)
        );
        for capacity in [1, 2, DEFAULT_DIRTY_CAPACITY, 1_048_576] {
            assert!(SegmentLayout::new(1, 1, 8, 8, capacity).is_ok());
        }
    }

    /// The dirty ring sits between the last event ring and the trailer, and the header says so.
    #[test]
    fn shm_the_dirty_ring_is_placed_between_the_event_rings_and_the_trailer() {
        let layout = layout();
        assert_eq!(
            layout.dirty_offset(),
            layout.event_offset()
                + layout.directory_capacity() as usize * layout.event_ring_bytes()
        );
        assert_eq!(
            layout.trailer_offset(),
            layout.dirty_offset() + layout.dirty_capacity() as usize * DIRTY_SLOT_BYTES
        );
        let (region, _writer) = formatted();
        let header = super::layout::header_record(region.cells()).expect("header record");
        assert_eq!(
            header
                .word64(super::layout::HDR_DIRTY_OFFSET)
                .relaxed_load(),
            layout.dirty_offset() as u64
        );
        assert_eq!(
            header
                .word32(super::layout::HDR_DIRTY_CAPACITY)
                .relaxed_load(),
            layout.dirty_capacity()
        );
        assert_eq!(
            header
                .word32(super::layout::HDR_DIRTY_STRIDE)
                .relaxed_load(),
            DIRTY_SLOT_BYTES as u32
        );
    }

    #[test]
    fn shm_install_refuses_a_duplicate_market_and_a_full_directory() {
        let (_region, mut writer) = formatted();
        assert!(writer.install(&market("a")).is_ok());
        assert_eq!(
            writer.install(&market("a")).err(),
            Some(WriterError::MarketAlreadyInstalled)
        );
        for slug in ["b", "c", "d"] {
            assert!(writer.install(&market(slug)).is_ok());
        }
        assert_eq!(
            writer.install(&market("e")).err(),
            Some(WriterError::DirectoryFull)
        );
    }
}
