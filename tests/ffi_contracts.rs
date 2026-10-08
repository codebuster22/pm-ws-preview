//! The C-ABI surface exercised as a same-crate consumer would call it: raw pointers over a
//! real file-backed segment, never through `src/shm/`'s own types directly.

use pm_ws::ffi::*;
use pm_ws::limitless::shard::{MarketOutcome, MarketRejection, MarketStatus};
use pm_ws::*;
use std::ffi::CStr;
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

const INSTANCE_ID: u128 = 0x2026_0901_dead_beef_cafe_f00d_1234_5678;
const SLUG: &str = "ffi-contract-market";

fn market_ref() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).unwrap(),
    )
}

/// A second market, so that a session-local index and a segment directory index can be told
/// apart: a session that resolves only this one holds `market` 0 for directory index 1.
fn second_market_ref() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), "ffi-contract-market-two").unwrap(),
    )
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn level(side: Side, price: &str, quantity: &str) -> Level {
    Level::new(
        side,
        Price::parse(price, grammar()).unwrap(),
        Quantity::parse(quantity, grammar()).unwrap(),
    )
}

fn provenance() -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market_ref(),
        outcome: None,
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-09-01T00:00:00.000Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new([], SourceEvidenceCapacity::new(0).unwrap())
            .unwrap(),
        daemon_generation: 3,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).unwrap(),
        subscription_generation: 7,
        receive_position: 0,
        commit_position: 0,
        local_receive_time: LocalMonotonicTimestamp::new(0),
        local_commit_time: LocalMonotonicTimestamp::new(0),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .unwrap()
}

fn base_snapshot() -> Candidate {
    Candidate::snapshot(
        provenance(),
        BoundedLevels::new(
            [
                level(Side::Bid, "0.400", "1000000"),
                level(Side::Ask, "0.600", "2000000"),
            ],
            LevelCapacity::new(8).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn delta_at(step: u64) -> Candidate {
    Candidate::source_delta(
        provenance(),
        BoundedLevels::new(
            [level(Side::Bid, "0.500", &format!("{}", 1_000_000 + step))],
            LevelCapacity::new(8).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn publish_commit(
    writer: &mut SegmentWriter,
    handle: MarketHandle,
    book: &OrderBook,
    commit: &BookCommit,
) {
    writer.publish(handle, &book.publish(), 0).unwrap();
    for record in commit.mutations() {
        writer
            .publish_mutation(
                handle,
                commit.revision(),
                record.cursor(),
                record.mutation(),
                0,
            )
            .unwrap();
    }
}

fn temp_segment_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-ffi-{tag}-{}-{}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

fn build_segment(
    tag: &str,
    directory: u32,
    slots: u32,
    level_capacity: u32,
    event_capacity: u32,
) -> (PathBuf, Arc<SegmentRegion>, SegmentWriter) {
    let layout = SegmentLayout::new(directory, slots, level_capacity, event_capacity, 16).unwrap();
    let path = temp_segment_path(tag);
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, INSTANCE_ID, 1),
    )
    .unwrap();
    (path, region, writer)
}

/// The sibling doorbell page a file-backed segment declares on this host, alongside its
/// segment file.
fn doorbell_page_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.doorbell", path.display()))
}

/// A segment created with its doorbell forced into the sibling page, deterministically and
/// regardless of what this host's own probe would decide — see [`DoorbellPlacement::ForcePage`].
fn build_page_segment(
    tag: &str,
    directory: u32,
    slots: u32,
    level_capacity: u32,
    event_capacity: u32,
) -> (PathBuf, Arc<SegmentRegion>, SegmentWriter) {
    let layout = SegmentLayout::new(directory, slots, level_capacity, event_capacity, 16).unwrap();
    let path = temp_segment_path(tag);
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig {
            doorbell: DoorbellPlacement::ForcePage,
            ..SegmentConfig::new(layout, INSTANCE_ID, 1)
        },
    )
    .unwrap();
    assert_eq!(writer.doorbell_feature_bit(), FEATURE_DOORBELL_PAGE);
    (path, region, writer)
}

/// # Safety obligations discharged by every helper below
/// Every raw pointer these helpers pass is either null (where this FFI's own contract
/// documents null as valid) or borrowed from a local, live Rust value for exactly the
/// duration of the call, matching each entry point's documented `# Safety` contract in
/// `src/ffi/mod.rs`.
fn open_session(path: &Path) -> *mut PmwsSession {
    let bytes = path.to_str().expect("utf8 path").as_bytes();
    let mut session: *mut PmwsSession = ptr::null_mut();
    let status = unsafe { pmws_open(bytes.as_ptr(), bytes.len(), &mut session) };
    assert_eq!(status, PMWS_STATUS_OK, "pmws_open failed");
    assert!(!session.is_null());
    session
}

fn resolve(session: *mut PmwsSession, market: &MarketRef) -> u32 {
    let venue = market.venue().as_str().as_bytes();
    let kind = market.key().kind().as_str().as_bytes();
    let key = market.key().value().as_bytes();
    let mut out = 0_u32;
    let status = unsafe {
        pmws_resolve(
            session,
            venue.as_ptr(),
            venue.len(),
            kind.as_ptr(),
            kind.len(),
            key.as_ptr(),
            key.len(),
            &mut out,
        )
    };
    assert_eq!(status, PMWS_STATUS_OK, "pmws_resolve failed");
    out
}

fn zeroed_state() -> PmwsState {
    // SAFETY: `PmwsState` is a plain-old-data `#[repr(C)]` struct of integers and byte
    // arrays; an all-zero bit pattern is a valid value for every field.
    unsafe { std::mem::zeroed() }
}
fn zeroed_level() -> PmwsLevel {
    // SAFETY: as `zeroed_state`.
    unsafe { std::mem::zeroed() }
}
fn zeroed_spans() -> PmwsIdentitySpans {
    // SAFETY: as `zeroed_state`.
    unsafe { std::mem::zeroed() }
}
fn zeroed_info() -> PmwsSegmentInfo {
    // SAFETY: as `zeroed_state`.
    unsafe { std::mem::zeroed() }
}

fn attach(
    session: *mut PmwsSession,
    market: u32,
    capacity: u32,
) -> (i32, PmwsState, Vec<PmwsLevel>) {
    let mut state = zeroed_state();
    let mut levels = vec![zeroed_level(); capacity as usize];
    let ptr = if capacity == 0 {
        ptr::null_mut()
    } else {
        levels.as_mut_ptr()
    };
    let status = unsafe { pmws_attach(session, market, &mut state, ptr, capacity) };
    (status, state, levels)
}

fn read_state(
    session: *mut PmwsSession,
    market: u32,
    capacity: u32,
) -> (i32, PmwsState, Vec<PmwsLevel>) {
    let mut state = zeroed_state();
    let mut levels = vec![zeroed_level(); capacity as usize];
    let ptr = if capacity == 0 {
        ptr::null_mut()
    } else {
        levels.as_mut_ptr()
    };
    let status = unsafe { pmws_read_state(session, market, &mut state, ptr, capacity) };
    (status, state, levels)
}

fn reattach(
    session: *mut PmwsSession,
    market: u32,
    capacity: u32,
) -> (i32, PmwsState, Vec<PmwsLevel>) {
    let mut state = zeroed_state();
    let mut levels = vec![zeroed_level(); capacity as usize];
    let ptr = if capacity == 0 {
        ptr::null_mut()
    } else {
        levels.as_mut_ptr()
    };
    let status = unsafe { pmws_reattach(session, market, &mut state, ptr, capacity) };
    (status, state, levels)
}

fn next_event(session: *mut PmwsSession, market: u32) -> (i32, PmwsEvent) {
    // SAFETY: `PmwsEvent` is plain-old-data; zeroed is a valid value.
    let mut event: PmwsEvent = unsafe { std::mem::zeroed() };
    let status = unsafe { pmws_next_event(session, market, &mut event) };
    (status, event)
}

fn decimal_text_of(decimal: &PmwsDecimal) -> String {
    let mut len = 0_usize;
    let status = unsafe { pmws_decimal_text(decimal, ptr::null_mut(), 0, &mut len) };
    assert_eq!(status, PMWS_STATUS_BUFFER_TOO_SMALL);
    let mut buf = vec![0_u8; len];
    let mut out_len = 0_usize;
    let status = unsafe { pmws_decimal_text(decimal, buf.as_mut_ptr(), buf.len(), &mut out_len) };
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(out_len, len);
    String::from_utf8(buf).unwrap()
}

#[test]
fn ffi_happy_path_reads_a_real_segment_exactly() {
    let (path, _region, mut writer) = build_segment("happy", 2, 2, 16, 64);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let mut spans = zeroed_spans();
    let mut buf = vec![0_u8; 64];
    assert_eq!(
        unsafe { pmws_market_identity(session, market, buf.as_mut_ptr(), buf.len(), &mut spans) },
        PMWS_STATUS_OK
    );
    assert_eq!(
        &buf[spans.venue_offset as usize..(spans.venue_offset + spans.venue_len) as usize],
        b"limitless"
    );
    assert_eq!(
        &buf[spans.kind_offset as usize..(spans.kind_offset + spans.kind_len) as usize],
        b"slug"
    );
    assert_eq!(
        &buf[spans.key_offset as usize..(spans.key_offset + spans.key_len) as usize],
        SLUG.as_bytes()
    );

    // Attach before the delta commits, so the delta's mutation is what `pmws_next_event`
    // below observes rather than something already folded into the attached snapshot.
    let published = book.publish();
    let (status, state, levels) = attach(session, market, 16);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(state.revision, published.revision());
    assert_eq!(state.authority_state, 4, "Live");
    assert_eq!(state.authority_reason, 0);
    assert_eq!(state.continuity_kind, 1, "Intact");
    assert_eq!(state.publication_present, 1);
    assert_eq!(state.origin, 1, "SourceReported");
    assert_eq!(state.representation, 1, "VenueNative");
    assert_eq!(
        &state.native_family[..state.native_family_len as usize],
        b"orderbookUpdate"
    );
    let canonical = published.canonical_levels();
    assert_eq!(state.level_count as usize, canonical.len());
    for (level, published) in levels[..state.level_count as usize].iter().zip(canonical) {
        assert_eq!(
            decimal_text_of(&level.price),
            published.price().value().canonical()
        );
        assert_eq!(
            decimal_text_of(&level.quantity),
            published.quantity().value().canonical()
        );
        assert_eq!(
            level.side,
            if published.side() == Side::Bid { 1 } else { 2 }
        );
    }

    let commit = book.apply_source_delta(&delta_at(1)).unwrap();
    assert_eq!(commit.mutations().len(), 1);
    publish_commit(&mut writer, handle, &book, &commit);

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(event.delivery_kind, 1);
    assert_eq!(event.continuity_reason, 0);
    assert_eq!(event.origin, 1, "SourceReported");
    assert_eq!(event.side, 1, "Bid");
    assert_eq!(decimal_text_of(&event.price), "0.5");
    assert_eq!(event.new_present, 1);
    assert_eq!(decimal_text_of(&event.new_quantity), "1000001");
    assert_eq!(event.daemon_generation, 3);
    assert_eq!(event.subscription_generation, 7);

    let (status, _) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_NONE);

    let mut info = zeroed_info();
    assert_eq!(
        unsafe { pmws_segment_info(session, &mut info) },
        PMWS_STATUS_OK
    );
    assert_eq!(info.instance_id_low, INSTANCE_ID as u64);
    assert_eq!(info.instance_id_high, (INSTANCE_ID >> 64) as u64);
    assert_eq!(info.directory_capacity, 2);
    assert_eq!(info.level_capacity, 16);
    assert_eq!(info.event_capacity, 64);
    assert!(info.publication_generation >= 2);
    assert_eq!(
        unsafe { pmws_publication_generation(session) },
        info.publication_generation
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_overrun_is_sticky_until_reattach_then_polling_resumes() {
    let (path, _region, mut writer) = build_segment("overrun", 1, 1, 8, 4);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());
    let (status, _, _) = attach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);

    for step in 0..10 {
        let commit = book.apply_source_delta(&delta_at(step)).unwrap();
        publish_commit(&mut writer, handle, &book, &commit);
    }

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_CONTINUITY_LOST);
    assert_eq!(event.delivery_kind, 2);
    assert_eq!(event.continuity_reason, 1, "BREAK_OVERRUN");
    assert_eq!(event.missed, 0);

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_CONTINUITY_LOST, "loss must be sticky");
    assert_eq!(event.continuity_reason, 1);

    let (status, state, _) = reattach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(state.revision, book.revision());

    let (status, _) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_NONE, "resumed at the tip, nothing new");

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_recovery_base_reports_the_recovery_base_reason() {
    let (path, _region, mut writer) = build_segment("rebase", 1, 1, 8, 64);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());
    let (status, _, _) = attach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);

    let commit = book.apply_source_delta(&delta_at(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let (status, _) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_OK);

    assert!(
        book.report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
            .unwrap()
    );
    let rebase = book.apply_snapshot(&base_snapshot()).unwrap();
    assert!(rebase.recovery_base());
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_CONTINUITY_LOST);
    assert_eq!(event.delivery_kind, 2);
    assert_eq!(event.continuity_reason, 5, "BREAK_RECOVERY_BASE");

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_buffer_too_small_reports_the_required_size_and_a_retry_succeeds() {
    let (path, _region, mut writer) = build_segment("small-buffer", 1, 1, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let (status, state, _) = attach(session, market, 0);
    assert_eq!(status, PMWS_STATUS_BUFFER_TOO_SMALL);
    assert_eq!(state.level_count, 0);
    assert!(state.level_capacity_required > 0);
    assert_eq!(state.revision, book.revision(), "scalars still fill");

    let required = state.level_capacity_required;
    let (status, state, _) = attach(session, market, required);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(state.level_count, required);

    let mut spans = zeroed_spans();
    assert_eq!(
        unsafe { pmws_market_identity(session, market, ptr::null_mut(), 0, &mut spans) },
        PMWS_STATUS_BUFFER_TOO_SMALL
    );
    let total = (spans.venue_len + spans.kind_len + spans.key_len) as usize;
    assert!(total > 0);
    let mut buf = vec![0_u8; total];
    assert_eq!(
        unsafe { pmws_market_identity(session, market, buf.as_mut_ptr(), buf.len(), &mut spans) },
        PMWS_STATUS_OK
    );

    let decimal = PmwsDecimal {
        coefficient_low: 12345,
        coefficient_high: 0,
        scale: 2,
        reserved: 0,
    };
    let mut len = 0_usize;
    assert_eq!(
        unsafe { pmws_decimal_text(&decimal, ptr::null_mut(), 0, &mut len) },
        PMWS_STATUS_BUFFER_TOO_SMALL
    );
    assert_eq!(len, "123.45".len());
    assert_eq!(decimal_text_of(&decimal), "123.45");

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_failed_attach_leaves_no_half_attachment() {
    let (path, _region, mut writer) = build_segment("failed-attach", 1, 1, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let (status, state, _) = attach(session, market, 0);
    assert_eq!(status, PMWS_STATUS_BUFFER_TOO_SMALL);
    assert!(state.level_capacity_required > 0);

    let (status, _) = next_event(session, market);
    assert_eq!(
        status, PMWS_STATUS_NOT_ATTACHED,
        "a failed attach with no prior stream must leave the session unattached, not \
         half-attached"
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

/// A too-small level buffer fails the reattach; the stream must stay exactly where it was
/// rather than silently resuming at the book's current tip.
#[test]
fn ffi_failed_reattach_leaves_the_cursor_unmoved() {
    let (path, _region, mut writer) = build_segment("failed-reattach", 1, 1, 8, 32);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());
    let (status, _, _) = attach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);

    let commit = book.apply_source_delta(&delta_at(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let commit = book.apply_source_delta(&delta_at(2)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);

    let (status, first) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(decimal_text_of(&first.new_quantity), "1000001");

    let (status, state, _) = reattach(session, market, 0);
    assert_eq!(status, PMWS_STATUS_BUFFER_TOO_SMALL);
    assert!(state.level_capacity_required > 0);

    let (status, second) = next_event(session, market);
    assert_eq!(
        status, PMWS_STATUS_OK,
        "a failed reattach must not move the stream's cursor"
    );
    assert_eq!(second.delivery_kind, 1);
    assert_eq!(second.cursor_epoch, first.cursor_epoch);
    assert_eq!(second.cursor_position, first.cursor_position + 1);
    assert_eq!(
        decimal_text_of(&second.new_quantity),
        "1000002",
        "must deliver exactly the event the unmoved cursor was already parked at"
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_failed_reattach_after_a_loss_keeps_it_sticky_until_one_succeeds() {
    let (path, _region, mut writer) = build_segment("sticky-loss", 1, 1, 8, 4);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());
    let (status, _, _) = attach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);

    for step in 0..10 {
        let commit = book.apply_source_delta(&delta_at(step)).unwrap();
        publish_commit(&mut writer, handle, &book, &commit);
    }

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_CONTINUITY_LOST);
    assert_eq!(event.continuity_reason, 1, "BREAK_OVERRUN");

    let (status, state, _) = reattach(session, market, 0);
    assert_eq!(status, PMWS_STATUS_BUFFER_TOO_SMALL);
    assert!(state.level_capacity_required > 0);

    let (status, event) = next_event(session, market);
    assert_eq!(
        status, PMWS_STATUS_CONTINUITY_LOST,
        "a failed reattach must not clear a sticky loss"
    );
    assert_eq!(event.continuity_reason, 1, "BREAK_OVERRUN");

    let (status, state, _) = reattach(session, market, 8);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(state.revision, book.revision());

    let (status, _) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_NONE, "resumed at the tip, nothing new");

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_null_pointers_and_bad_utf8_are_invalid_argument() {
    let bad_utf8: &[u8] = &[0xff, 0xfe];

    let mut session: *mut PmwsSession = ptr::null_mut();
    assert_eq!(
        unsafe { pmws_open(ptr::null(), 1, &mut session) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    let ok_bytes = b"/does/not/matter";
    assert_eq!(
        unsafe { pmws_open(ok_bytes.as_ptr(), ok_bytes.len(), ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_open(ok_bytes.as_ptr(), 0, &mut session) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a zero-length required string is refused"
    );
    assert_eq!(
        unsafe { pmws_open(bad_utf8.as_ptr(), bad_utf8.len(), &mut session) },
        PMWS_STATUS_INVALID_ARGUMENT
    );

    let (path, _region, mut writer) = build_segment("nullargs", 1, 1, 4, 4);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let mut out = 0_u32;
    let venue = b"limitless";
    let kind = b"slug";
    let key = b"x";
    assert_eq!(
        unsafe {
            pmws_resolve(
                session,
                ptr::null(),
                1,
                kind.as_ptr(),
                kind.len(),
                key.as_ptr(),
                key.len(),
                &mut out,
            )
        },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe {
            pmws_resolve(
                session,
                venue.as_ptr(),
                venue.len(),
                kind.as_ptr(),
                kind.len(),
                key.as_ptr(),
                key.len(),
                ptr::null_mut(),
            )
        },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe {
            pmws_resolve(
                session,
                bad_utf8.as_ptr(),
                bad_utf8.len(),
                kind.as_ptr(),
                kind.len(),
                key.as_ptr(),
                key.len(),
                &mut out,
            )
        },
        PMWS_STATUS_INVALID_ARGUMENT
    );

    let mut levels = vec![zeroed_level(); 4];
    assert_eq!(
        unsafe { pmws_attach(session, market, ptr::null_mut(), levels.as_mut_ptr(), 4) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_read_state(session, market, ptr::null_mut(), levels.as_mut_ptr(), 4) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_reattach(session, market, ptr::null_mut(), levels.as_mut_ptr(), 4) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_next_event(session, market, ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_segment_info(session, ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    let mut throwaway_info = zeroed_info();
    assert_eq!(
        unsafe { pmws_segment_info(ptr::null(), &mut throwaway_info) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_market_identity(session, market, ptr::null_mut(), 0, ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    let decimal = PmwsDecimal {
        coefficient_low: 1,
        coefficient_high: 0,
        scale: 0,
        reserved: 0,
    };
    let mut throwaway_len = 0_usize;
    assert_eq!(
        unsafe { pmws_decimal_text(ptr::null(), ptr::null_mut(), 0, &mut throwaway_len) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_decimal_text(&decimal, ptr::null_mut(), 0, ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(unsafe { pmws_publication_generation(ptr::null()) }, 0);

    unsafe { pmws_close(session) };
    unsafe { pmws_close(ptr::null_mut()) };
    let _ = std::fs::remove_file(&path);
}

/// A session opened from a path holds no lease, and renewing one is a caller error rather
/// than a silent success.
///
/// The distinction matters because the two ways to make a session look identical from JS or
/// Python afterwards: only the one that came through a daemon's control socket has a
/// connection to renew, and a renewal that quietly did nothing would let a consumer believe
/// its leases were being kept alive.
#[test]
fn ffi_renew_needs_a_session_that_holds_a_lease() {
    let (path, _region, mut writer) = build_segment("renew", 1, 1, 4, 4);
    let _handle = writer.install(&market_ref()).unwrap();
    let session = open_session(&path);

    assert_eq!(
        unsafe { pmws_renew(ptr::null()) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a null session is a caller error"
    );
    assert_eq!(
        unsafe { pmws_renew(session) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a session opened from a path has no control connection and no lease to renew"
    );

    unsafe { pmws_close(session) };
    let _removed = std::fs::remove_file(&path);
    let _removed = std::fs::remove_file(doorbell_page_path(&path));
}

#[test]
fn ffi_reports_not_attached_and_no_published_state() {
    let (path, _region, mut writer) = build_segment("unpublished", 1, 1, 4, 4);
    let _handle = writer.install(&market_ref()).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let (status, _, _) = read_state(session, market, 0);
    assert_eq!(status, PMWS_STATUS_NO_PUBLISHED_STATE);

    let (status, _) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_NOT_ATTACHED);

    let (status, _, _) = reattach(session, market, 0);
    assert_eq!(status, PMWS_STATUS_NOT_ATTACHED);

    let missing = MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), "does-not-exist").unwrap(),
    );
    let venue = missing.venue().as_str().as_bytes();
    let kind = missing.key().kind().as_str().as_bytes();
    let key = missing.key().value().as_bytes();
    let mut out = 0_u32;
    let status = unsafe {
        pmws_resolve(
            session,
            venue.as_ptr(),
            venue.len(),
            kind.as_ptr(),
            kind.len(),
            key.as_ptr(),
            key.len(),
            &mut out,
        )
    };
    assert_eq!(status, PMWS_STATUS_MARKET_NOT_FOUND);

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ffi_open_reports_segment_incompatible_and_io() {
    let truncated = temp_segment_path("truncated");
    let _region = SegmentRegion::create_file(&truncated, 256).unwrap();
    let mut session: *mut PmwsSession = ptr::null_mut();
    let bytes = truncated.to_str().unwrap().as_bytes();
    assert_eq!(
        unsafe { pmws_open(bytes.as_ptr(), bytes.len(), &mut session) },
        PMWS_STATUS_SEGMENT_INCOMPATIBLE,
        "a region too small for the header and trailer"
    );
    let _ = std::fs::remove_file(&truncated);

    let unformatted = temp_segment_path("unformatted");
    let layout = SegmentLayout::new(1, 1, 4, 4, 16).unwrap();
    let _region = SegmentRegion::create_file(&unformatted, layout.region_size()).unwrap();
    let bytes = unformatted.to_str().unwrap().as_bytes();
    assert_eq!(
        unsafe { pmws_open(bytes.as_ptr(), bytes.len(), &mut session) },
        PMWS_STATUS_SEGMENT_INCOMPATIBLE,
        "a correctly sized but never-formatted (zero-magic) segment"
    );
    let _ = std::fs::remove_file(&unformatted);

    let missing = temp_segment_path("missing");
    let bytes = missing.to_str().unwrap().as_bytes();
    assert_eq!(
        unsafe { pmws_open(bytes.as_ptr(), bytes.len(), &mut session) },
        PMWS_STATUS_IO
    );
}

// No entry point in `src/ffi/mod.rs` panics on any input this suite can construct; every
// fallible step returns a typed status. `guarded`'s panic-to-status conversion is covered by
// `ffi_guarded_converts_a_panic_into_the_default` in `src/ffi/mod.rs`.

#[test]
fn ffi_decimal_text_endpoint_matches_the_accept_and_reject_vectors() {
    let widest = (1_u128 << 127) - 1;
    let (low, high) = (widest as u64, (widest >> 64) as u64);
    let accepted = [
        ((0, 0, 0), "0"),
        ((0, 0, 5), "0"),
        ((1200, 0, 2), "12"),
        ((low, high, 0), "170141183460469231731687303715884105727"),
        ((low, high, 38), "1.70141183460469231731687303715884105727"),
    ];
    for ((coefficient_low, coefficient_high, scale), expected) in accepted {
        let decimal = PmwsDecimal {
            coefficient_low,
            coefficient_high,
            scale,
            reserved: 0,
        };
        assert_eq!(decimal_text_of(&decimal), expected);
    }
    let rejected = [
        (0, 1_u64 << 63, 0),
        (low, high | (1_u64 << 63), 0),
        (1, 0, u32::from(u16::MAX) + 1),
        (1, 0, u32::MAX),
    ];
    for (coefficient_low, coefficient_high, scale) in rejected {
        let decimal = PmwsDecimal {
            coefficient_low,
            coefficient_high,
            scale,
            reserved: 0,
        };
        let mut len = 0_usize;
        assert_eq!(
            unsafe { pmws_decimal_text(&decimal, ptr::null_mut(), 0, &mut len) },
            PMWS_STATUS_INVALID_ARGUMENT,
            "decimal {coefficient_low} {coefficient_high} {scale} was accepted"
        );
    }
}

#[test]
fn ffi_struct_sizes_and_alignments_are_pinned() {
    use std::mem::{align_of, size_of};
    assert_eq!(
        (size_of::<PmwsDecimal>(), align_of::<PmwsDecimal>()),
        (24, 8)
    );
    assert_eq!((size_of::<PmwsLevel>(), align_of::<PmwsLevel>()), (56, 8));
    assert_eq!((size_of::<PmwsState>(), align_of::<PmwsState>()), (160, 8));
    assert_eq!((size_of::<PmwsEvent>(), align_of::<PmwsEvent>()), (392, 8));
    assert_eq!(
        (size_of::<PmwsSegmentInfo>(), align_of::<PmwsSegmentInfo>()),
        (48, 8)
    );
    assert_eq!(
        (
            size_of::<PmwsIdentitySpans>(),
            align_of::<PmwsIdentitySpans>()
        ),
        (24, 4)
    );
    // `arrival_time` lands with no gap after `PmwsState`'s 152-byte tail; `PmwsEvent`'s tail
    // is 4 bytes short of 8-alignment, so `reserved3` fills exactly that gap before
    // `arrival_time`.
    assert_eq!(std::mem::offset_of!(PmwsState, arrival_time), 152);
    assert_eq!(std::mem::offset_of!(PmwsEvent, reserved3), 380);
    assert_eq!(std::mem::offset_of!(PmwsEvent, arrival_time), 384);
}

/// `pmws_connect` refuses what it cannot use before it opens a socket, and a control socket
/// nothing is listening on is a typed I/O failure rather than a hang, a panic, or a session.
///
/// The accepting path needs a live daemon and is proved in `tests/daemon_contracts.rs`; what
/// this pins is the boundary contract every entry point of this ABI shares — a null paired
/// with a non-zero length, an empty string, and a null out-parameter are all refused, and
/// `*out` is never written on a failure.
#[test]
fn ffi_connect_refuses_bad_arguments_and_an_unreachable_daemon() {
    let socket = format!("/tmp/pmws-connect-absent-{}.sock", std::process::id());
    let market = "btc-up-or-down-5-min-1788172500";
    let mut session: *mut PmwsSession = ptr::null_mut();
    unsafe {
        assert_eq!(
            pmws_connect(
                ptr::null(),
                0,
                market.as_ptr(),
                market.len(),
                &raw mut session
            ),
            PMWS_STATUS_INVALID_ARGUMENT,
            "a control socket path is required"
        );
        assert_eq!(
            pmws_connect(
                socket.as_ptr(),
                socket.len(),
                ptr::null(),
                0,
                &raw mut session
            ),
            PMWS_STATUS_INVALID_ARGUMENT,
            "a market is required"
        );
        assert_eq!(
            pmws_connect(
                socket.as_ptr(),
                socket.len(),
                market.as_ptr(),
                market.len(),
                ptr::null_mut()
            ),
            PMWS_STATUS_INVALID_ARGUMENT,
            "an out-parameter is required"
        );
        assert_eq!(
            pmws_connect(
                socket.as_ptr(),
                socket.len(),
                market.as_ptr(),
                market.len(),
                &raw mut session
            ),
            PMWS_STATUS_IO,
            "a socket nothing listens on is an i/o failure"
        );
    }
    assert!(
        session.is_null(),
        "no session handle is written on any failure"
    );
}

/// One attach conversation against a scripted server: it answers with `attachment` and
/// transfers `sends` copies of the segment at `segment`, and this returns what `pmws_connect`
/// made of it.
///
/// Scripted rather than driven by the real daemon because what is under test is the
/// *receiver's* rule — a daemon transfers one descriptor, so a second one is a shape only a
/// peer of some other vintage produces, and this is the only way to present one.
fn connect_against(tag: &str, segment: &Path, attachment: Attachment, sends: usize) -> i32 {
    let path = PathBuf::from(format!(
        "/tmp/pmws-connect-{tag}-{}.sock",
        std::process::id()
    ));
    let _removed = std::fs::remove_file(path.as_path());
    let listener =
        std::os::unix::net::UnixListener::bind(path.as_path()).expect("the fake daemon binds");
    let line =
        pm_ws::encode_line(&ControlResponse::Attached { attachment }).expect("the answer encodes");
    let opened = std::fs::File::open(segment).expect("the segment opens read-only");
    let server = std::thread::spawn(move || {
        let Ok((mut stream, _address)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 1024];
        let mut filled = 0;
        while filled < request.len() && !request[..filled].contains(&b'\n') {
            match std::io::Read::read(&mut stream, &mut request[filled..]) {
                Ok(0) | Err(_) => break,
                Ok(read) => filled += read,
            }
        }
        let descriptors: Vec<std::os::fd::BorrowedFd<'_>> =
            std::iter::repeat_n(opened.as_fd(), sends).collect();
        let sent = pm_ws::send_with_fds(stream.as_fd(), line.as_bytes(), descriptors.as_slice())
            .expect("the scripted answer is sent");
        assert_eq!(sent, line.len(), "the whole answer rides one message");
        let mut drained = [0_u8; 1];
        let _closed = std::io::Read::read(&mut stream, &mut drained);
    });

    let socket = path.to_str().expect("the path is utf-8").to_owned();
    let market = "btc-up-or-down-5-min-1788172500";
    let mut session: *mut PmwsSession = ptr::null_mut();
    let status = unsafe {
        pmws_connect(
            socket.as_ptr(),
            socket.len(),
            market.as_ptr(),
            market.len(),
            &raw mut session,
        )
    };
    if session.is_null() {
        assert!(status < 0, "no session handle means a typed failure");
    } else {
        unsafe { pmws_close(session) };
    }
    server.join().expect("the scripted server finishes");
    let _removed = std::fs::remove_file(path.as_path());
    status
}

/// An accepted attach carries the segment's descriptor and nothing else, a reply carrying a
/// second one is refused rather than partly adopted, and a reply whose doorbell claim the
/// header refutes is refused too.
///
/// The doorbell page's descriptor is never transferred — it is a writable-length object, so a
/// consumer holding it could truncate a file the daemon stores through — so a page-placement
/// attachment arrives with exactly as many descriptors as a header-placement one does. What
/// the placement still decides is whether the consumer may park, which is why it is checked
/// against the validated header rather than believed: the promise here is built from the
/// writer's own answer, because which placement a segment takes is the platform's decision and
/// not a knob a test may set.
#[test]
fn ffi_connect_takes_one_descriptor_and_refuses_a_second() {
    let (path, _region, writer) = build_segment("connect-shape", 4, 4, 8, 8);
    let declared = if writer.doorbell_feature_bit() == FEATURE_DOORBELL_PAGE {
        DoorbellLocation::Page
    } else {
        DoorbellLocation::InHeader
    };
    let refuted = if declared == DoorbellLocation::Page {
        DoorbellLocation::InHeader
    } else {
        DoorbellLocation::Page
    };
    let promise = |descriptors: u8, doorbell: DoorbellLocation| Attachment {
        shard: 0,
        segment: "scripted.seg".to_owned(),
        instance_id: format!("{INSTANCE_ID:032x}"),
        segment_generation: 1,
        doorbell,
        descriptors,
        lease_ttl_ms: 0,
    };

    assert_eq!(
        connect_against("shape-one", path.as_path(), promise(1, declared), 1),
        PMWS_STATUS_OK,
        "one promised descriptor and one transferred is the whole shape of an accepted attach, \
         whichever placement this platform's segment declares"
    );
    assert_eq!(
        connect_against("shape-two", path.as_path(), promise(2, declared), 2),
        PMWS_STATUS_ATTACH_INCOMPLETE,
        "a second descriptor is a promise this ABI no longer makes and never adopts"
    );
    assert_eq!(
        connect_against("shape-short", path.as_path(), promise(1, declared), 0),
        PMWS_STATUS_ATTACH_INCOMPLETE,
        "an answer that promised a descriptor and transferred none is incomplete"
    );
    assert_eq!(
        connect_against("shape-doorbell", path.as_path(), promise(1, refuted), 1),
        PMWS_STATUS_SEGMENT_INCOMPATIBLE,
        "a doorbell placement the header refutes describes some other segment, and is refused \
         on the same footing as a wrong instance or generation"
    );

    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// The attachment conversation is bounded by one deadline over the whole of it, not by a
/// timeout the peer can reset.
///
/// The server here accepts and then dribbles one byte a second and never a newline, which is
/// exactly what a per-receive timeout does not bound: every byte lands inside the limit, so
/// each read succeeds and the next one starts a fresh five seconds. Against the 64 KiB line
/// buffer that stretches one `pmws_connect` call — on a consumer's own thread, with no runtime
/// to cancel it — to days. What is asserted is therefore the wall clock as much as the status:
/// the call must give up around its own bound, having actually waited (so a lower bound is
/// asserted too, or an unrelated instant failure would pass), and hand back no session.
#[test]
fn ffi_connect_gives_up_on_a_peer_that_dribbles_bytes_forever() {
    let path = PathBuf::from(format!(
        "/tmp/pmws-connect-dribble-{}.sock",
        std::process::id()
    ));
    let _removed = std::fs::remove_file(path.as_path());
    let listener =
        std::os::unix::net::UnixListener::bind(path.as_path()).expect("the fake daemon binds");
    let server = std::thread::spawn(move || {
        let Ok((mut stream, _address)) = listener.accept() else {
            return;
        };
        for _ in 0..60 {
            if std::io::Write::write_all(&mut stream, b"x").is_err() {
                return;
            }
            let _flushed = std::io::Write::flush(&mut stream);
            std::thread::sleep(Duration::from_secs(1));
        }
    });

    let socket = path.to_str().expect("the path is utf-8").to_owned();
    let market = "btc-up-or-down-5-min-1788172500";
    let mut session: *mut PmwsSession = ptr::null_mut();
    let started = Instant::now();
    let status = unsafe {
        pmws_connect(
            socket.as_ptr(),
            socket.len(),
            market.as_ptr(),
            market.len(),
            &raw mut session,
        )
    };
    let elapsed = started.elapsed();

    assert_eq!(
        status, PMWS_STATUS_IO,
        "a conversation that never finishes is a typed i/o failure"
    );
    assert!(
        session.is_null(),
        "no session handle is written on a failure"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "the call waits out its own deadline rather than failing for some other reason: \
         {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "one deadline covers the whole conversation, so a peer that keeps sending cannot \
         extend it: {elapsed:?}"
    );

    let _joined = server.join();
    let _removed = std::fs::remove_file(path.as_path());
}

/// One step of a scripted control conversation: the next request line is taken, and then
/// either answered or left hanging.
struct Step {
    /// The answer line, or `None` for a step that takes the request and never answers it,
    /// holding the connection open until the client gives up on its own deadline. A script
    /// ends at such a step.
    answer: Option<String>,
    descriptors: usize,
    delay: Duration,
}

impl Step {
    /// Answers with a line that carries no descriptors, as every non-attaching answer does.
    fn answering(response: &ControlResponse) -> Self {
        Self {
            answer: Some(scripted_line(response)),
            descriptors: 0,
            delay: Duration::ZERO,
        }
    }

    /// Answers with a line carrying one copy of the segment's descriptor, as an accepted
    /// attach does.
    fn attaching(response: &ControlResponse) -> Self {
        Self {
            answer: Some(scripted_line(response)),
            descriptors: 1,
            delay: Duration::ZERO,
        }
    }

    /// Takes the request and never answers it: the daemon a client has to time out against.
    fn stalling() -> Self {
        Self {
            answer: None,
            descriptors: 0,
            delay: Duration::ZERO,
        }
    }

    /// Waits this long before answering, which is how a conversation is made to spend a
    /// chosen share of the client's one deadline.
    fn after(self, delay: Duration) -> Self {
        Self { delay, ..self }
    }
}

/// What one scripted conversation saw: every request line the FFI sent, and whether the
/// client closed the connection rather than leaving it open.
struct Conversation {
    asked: Vec<String>,
    closed_by_peer: bool,
}

/// One scripted control conversation over a single accepted connection: every request line
/// the FFI sends is answered by the next [`Step`] of `script`, which transfers that many
/// copies of the segment at `segment`.
///
/// The request lines are handed back when the server joins, because half of a lease's
/// contract is what it *sent*: a rollback release that never left the process is invisible in
/// the returned status alone, since a scripted daemon answers whatever it is asked. Lines
/// that arrive after the script has run out are recorded too, so a call that should have sent
/// nothing at all is caught sending something.
///
/// `closed_by_peer` is the other half: it is how a client dropping its control connection —
/// the thing that makes a daemon release every lease the session held — is observed from the
/// daemon's side rather than inferred.
fn scripted_daemon(
    tag: &str,
    segment: &Path,
    script: Vec<Step>,
) -> (PathBuf, std::thread::JoinHandle<Conversation>) {
    let path = PathBuf::from(format!(
        "/tmp/pmws-scripted-{tag}-{}.sock",
        std::process::id()
    ));
    let _removed = std::fs::remove_file(path.as_path());
    let listener =
        std::os::unix::net::UnixListener::bind(path.as_path()).expect("the fake daemon binds");
    let opened = std::fs::File::open(segment).expect("the segment opens read-only");
    let server = std::thread::spawn(move || {
        let mut conversation = Conversation {
            asked: Vec::new(),
            closed_by_peer: false,
        };
        let Ok((mut stream, _address)) = listener.accept() else {
            return conversation;
        };
        for step in script {
            let Some(line) = read_request_line(&mut stream) else {
                return conversation;
            };
            conversation.asked.push(line);
            let Some(answer) = step.answer else {
                break;
            };
            std::thread::sleep(step.delay);
            let transferred: Vec<std::os::fd::BorrowedFd<'_>> =
                std::iter::repeat_n(opened.as_fd(), step.descriptors).collect();
            if pm_ws::send_with_fds(stream.as_fd(), answer.as_bytes(), transferred.as_slice())
                .is_err()
            {
                return conversation;
            }
        }
        let mut trailing: Vec<u8> = Vec::new();
        loop {
            let mut chunk = [0_u8; 256];
            match std::io::Read::read(&mut stream, &mut chunk) {
                Ok(0) => {
                    conversation.closed_by_peer = true;
                    break;
                }
                Ok(read) => trailing.extend_from_slice(&chunk[..read]),
                Err(_) => break,
            }
        }
        for line in trailing.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            conversation
                .asked
                .push(String::from_utf8(line.to_vec()).expect("the request line is utf-8"));
        }
        conversation
    });
    (path, server)
}

/// Reads one newline-terminated request line, or `None` once the peer stops sending.
fn read_request_line(stream: &mut std::os::unix::net::UnixStream) -> Option<String> {
    let mut line = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        match std::io::Read::read(stream, &mut byte) {
            Ok(1) if byte[0] == b'\n' => break,
            Ok(1) => line.push(byte[0]),
            Ok(_) | Err(_) => return None,
        }
    }
    Some(String::from_utf8(line).expect("the request line is utf-8"))
}

fn scripted_line(response: &ControlResponse) -> String {
    pm_ws::encode_line(response).expect("the scripted answer encodes")
}

fn connect_scripted(socket: &Path, market: &str) -> *mut PmwsSession {
    let text = socket.to_str().expect("the socket path is utf-8");
    let mut session: *mut PmwsSession = ptr::null_mut();
    let status = unsafe {
        pmws_connect(
            text.as_ptr(),
            text.len(),
            market.as_ptr(),
            market.len(),
            &raw mut session,
        )
    };
    assert_eq!(status, PMWS_STATUS_OK, "the scripted attach is accepted");
    assert!(!session.is_null());
    session
}

fn lease(session: *const PmwsSession, market: &str) -> i32 {
    unsafe { pmws_lease(session, market.as_ptr(), market.len()) }
}

fn release(session: *const PmwsSession, market: &str) -> i32 {
    unsafe { pmws_release(session, market.as_ptr(), market.len()) }
}

fn requests(asked: &[String]) -> Vec<ControlRequest> {
    asked
        .iter()
        .map(|line| serde_json::from_str::<ControlRequest>(line).expect("the request decodes"))
        .collect()
}

/// The attachment a scripted daemon promises for `segment`, with the identity the segment
/// under test actually declares.
fn scripted_attachment(segment: &str, doorbell: DoorbellLocation) -> Attachment {
    Attachment {
        shard: 0,
        segment: segment.to_owned(),
        instance_id: format!("{INSTANCE_ID:032x}"),
        segment_generation: 1,
        doorbell,
        descriptors: 1,
        lease_ttl_ms: 0,
    }
}

fn declared_doorbell(writer: &SegmentWriter) -> DoorbellLocation {
    if writer.doorbell_feature_bit() == FEATURE_DOORBELL_PAGE {
        DoorbellLocation::Page
    } else {
        DoorbellLocation::InHeader
    }
}

/// A session takes further market leases on the connection it already holds, hands one back
/// without giving up the others, and goes on reading the segment it mapped.
///
/// Scripted for the reason `connect_against` is: what this pins is the *client's* half of the
/// conversation — which line each call sends, and what it makes of the answer — including two
/// shapes a real daemon will not produce on demand, a repeated release and a refused
/// identifier. Releasing the market the session connected with is the case that matters most:
/// the lease and the mapping are separate lifetimes, and the mapping is the one this call
/// must not touch.
#[test]
fn ffi_leases_another_market_and_releases_one_without_losing_the_mapping() {
    let (path, _region, mut writer) = build_segment("lease-release", 4, 4, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let doorbell = declared_doorbell(&writer);

    let refused = ControlResponse::Markets {
        markets: vec![MarketOutcome {
            slug: "not a slug".to_owned(),
            status: MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
        }],
    };
    let script = vec![
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-0.seg", doorbell),
        }),
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-0.seg", doorbell),
        }),
        Step::answering(&ControlResponse::Released { leases: 1 }),
        Step::answering(&ControlResponse::Released { leases: 1 }),
        Step::answering(&refused),
        Step::answering(&ControlResponse::Renewed { leases: 1 }),
    ];
    let (socket, server) = scripted_daemon("lease-release", path.as_path(), script);
    let session = connect_scripted(socket.as_path(), SLUG);

    assert_eq!(
        lease(session, "ffi-contract-market-two"),
        PMWS_STATUS_OK,
        "a second market in this session's own segment is a further lease on the same \
         connection, and the mapping it already holds covers it"
    );
    assert_eq!(
        release(session, SLUG),
        PMWS_STATUS_OK,
        "releasing the market this session connected with is legal"
    );
    assert_eq!(
        release(session, SLUG),
        PMWS_STATUS_OK,
        "a release the session no longer holds a lease for is idempotent, not an error"
    );
    assert_eq!(
        release(session, "not a slug"),
        PMWS_STATUS_INVALID_ARGUMENT,
        "an identifier no shard would accept is refused exactly as an attach's is"
    );
    assert_eq!(
        unsafe { pmws_renew(session) },
        PMWS_STATUS_OK,
        "every conversation reads its own answer, so the socket is left clean for the next"
    );

    let market = resolve(session, &market_ref());
    let (status, state, _levels) = read_state(session, market, 8);
    assert_eq!(
        status, PMWS_STATUS_OK,
        "the mapping outlives the lease: a released market is still readable"
    );
    assert_eq!(state.level_count, 2, "the published book is still there");
    assert_eq!(state.revision, book.revision());

    unsafe { pmws_close(session) };
    let conversation = server.join().expect("the scripted daemon finishes");
    let asked = requests(&conversation.asked);
    assert_eq!(
        asked,
        vec![
            ControlRequest::Attach {
                market: SLUG.to_owned()
            },
            ControlRequest::Attach {
                market: "ffi-contract-market-two".to_owned()
            },
            ControlRequest::Release {
                market: SLUG.to_owned()
            },
            ControlRequest::Release {
                market: SLUG.to_owned()
            },
            ControlRequest::Release {
                market: "not a slug".to_owned()
            },
            ControlRequest::Renew,
        ],
        "each call sends exactly its own line, in order"
    );
    let _removed = std::fs::remove_file(socket.as_path());
    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// A lease whose attachment names some other shard's segment is refused, and the lease the
/// daemon just granted is handed straight back rather than left orphaned on the session.
///
/// The two shards of one daemon share an instance identity and, freshly started, a segment
/// generation too, so the segment's own name is the only field that can tell them apart —
/// and it is the only field this test varies, or a check that ignored it would pass anyway.
/// The refusal is neither of its neighbours: the transfer arrived intact, so it is not
/// `ATTACH_INCOMPLETE`, and the segment named is a perfectly valid one this session simply
/// does not map, so it is not `SEGMENT_INCOMPATIBLE`.
#[test]
fn ffi_lease_rolls_back_an_attachment_naming_another_shards_segment() {
    let (path, _region, mut writer) = build_segment("lease-foreign", 4, 4, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let doorbell = declared_doorbell(&writer);

    let script = vec![
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-0.seg", doorbell),
        }),
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-1.seg", doorbell),
        }),
        Step::answering(&ControlResponse::Released { leases: 1 }),
        Step::answering(&ControlResponse::Renewed { leases: 1 }),
    ];
    let (socket, server) = scripted_daemon("lease-foreign", path.as_path(), script);
    let session = connect_scripted(socket.as_path(), SLUG);

    assert_eq!(
        lease(session, "elsewhere"),
        PMWS_STATUS_FOREIGN_SEGMENT,
        "a market another shard holds cannot be read through this session's mapping"
    );
    assert_eq!(
        unsafe { pmws_renew(session) },
        PMWS_STATUS_OK,
        "the rollback read its own answer, so the connection is still usable"
    );
    let market = resolve(session, &market_ref());
    let (status, _, _) = read_state(session, market, 8);
    assert_eq!(
        status, PMWS_STATUS_OK,
        "the session's own mapping is intact"
    );

    unsafe { pmws_close(session) };
    let conversation = server.join().expect("the scripted daemon finishes");
    let asked = requests(&conversation.asked);
    assert_eq!(
        asked,
        vec![
            ControlRequest::Attach {
                market: SLUG.to_owned()
            },
            ControlRequest::Attach {
                market: "elsewhere".to_owned()
            },
            ControlRequest::Release {
                market: "elsewhere".to_owned()
            },
            ControlRequest::Renew,
        ],
        "the refused lease is given back on the same connection it was taken on"
    );
    let _removed = std::fs::remove_file(socket.as_path());
    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// A foreign attachment whose rollback the daemon does not confirm leaves the demand this
/// call took unaccounted for, so the control connection is dropped rather than kept — which
/// is what makes the daemon release every lease the session held, the one this call should
/// never have taken included.
///
/// Two shapes of unconfirmed rollback, because both are answers a daemon really gives: an
/// `Error` line, which is what a daemon that does not know `release` at all answers, and a
/// per-market refusal, which is a complete conversation but still not the confirmation the
/// rollback needed. Neither is `FOREIGN_SEGMENT`: that status promises a session that is
/// still usable and a lease that is provably gone, and after an unconfirmed rollback neither
/// is true, so it is the `IO` every other lost connection on this ABI reports.
#[test]
fn ffi_lease_poisons_the_connection_when_a_rollback_is_not_confirmed() {
    let (path, _region, mut writer) = build_segment("rollback-unconfirmed", 4, 4, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let doorbell = declared_doorbell(&writer);

    let unconfirmed = vec![
        (
            "rollback-error",
            ControlResponse::Error {
                message: "unreadable request: unknown variant `release`".to_owned(),
            },
        ),
        (
            "rollback-refused",
            ControlResponse::Markets {
                markets: vec![MarketOutcome {
                    slug: "elsewhere".to_owned(),
                    status: MarketStatus::Removed,
                }],
            },
        ),
    ];

    for (tag, rollback) in unconfirmed {
        let script = vec![
            Step::attaching(&ControlResponse::Attached {
                attachment: scripted_attachment("shard-0.seg", doorbell),
            }),
            Step::attaching(&ControlResponse::Attached {
                attachment: scripted_attachment("shard-1.seg", doorbell),
            }),
            Step::answering(&rollback),
        ];
        let (socket, server) = scripted_daemon(tag, path.as_path(), script);
        let session = connect_scripted(socket.as_path(), SLUG);

        assert_eq!(
            lease(session, "elsewhere"),
            PMWS_STATUS_IO,
            "a rollback the daemon did not confirm is a lost connection, not the \
             foreign-segment refusal a confirmed one is: {tag}"
        );
        assert_eq!(
            unsafe { pmws_renew(session) },
            PMWS_STATUS_INVALID_ARGUMENT,
            "the control connection is gone, so this session answers exactly as one opened \
             from a path does: {tag}"
        );
        assert_eq!(
            release(session, SLUG),
            PMWS_STATUS_INVALID_ARGUMENT,
            "there is no connection left to give a lease back on: {tag}"
        );
        assert_eq!(
            lease(session, "ffi-contract-market-two"),
            PMWS_STATUS_INVALID_ARGUMENT,
            "and none to take a further lease on: {tag}"
        );

        let market = resolve(session, &market_ref());
        let (status, state, _levels) = read_state(session, market, 8);
        assert_eq!(
            status, PMWS_STATUS_OK,
            "the mapping is not what failed: it stays readable exactly as it does after \
             pmws_close: {tag}"
        );
        assert_eq!(
            state.level_count, 2,
            "the published book is still there: {tag}"
        );

        unsafe { pmws_close(session) };
        let conversation = server.join().expect("the scripted daemon finishes");
        assert!(
            conversation.closed_by_peer,
            "the daemon sees the connection close, which is what releases the leases it \
             still thinks this session holds: {tag}"
        );
        assert_eq!(
            requests(&conversation.asked),
            vec![
                ControlRequest::Attach {
                    market: SLUG.to_owned()
                },
                ControlRequest::Attach {
                    market: "elsewhere".to_owned()
                },
                ControlRequest::Release {
                    market: "elsewhere".to_owned()
                },
            ],
            "the rollback was attempted, and nothing was sent after it: {tag}"
        );
        let _removed = std::fs::remove_file(socket.as_path());
    }

    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// A release the daemon never answers ends the control connection rather than leaving it
/// open with an answer still owed on it.
///
/// A socket kept after a timed-out request is a socket whose next answer belongs to the
/// request before it: every later conversation on it would read one answer too early, for as
/// long as the session lived. Dropping it costs this session its leases, which is the honest
/// price, and the daemon learns of it the moment the peer closes.
#[test]
fn ffi_release_that_times_out_takes_the_control_connection_with_it() {
    let (path, _region, mut writer) = build_segment("release-timeout", 4, 4, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let doorbell = declared_doorbell(&writer);

    let script = vec![
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-0.seg", doorbell),
        }),
        Step::stalling(),
    ];
    let (socket, server) = scripted_daemon("release-timeout", path.as_path(), script);
    let session = connect_scripted(socket.as_path(), SLUG);

    let started = Instant::now();
    let status = release(session, SLUG);
    let elapsed = started.elapsed();
    assert_eq!(
        status, PMWS_STATUS_IO,
        "a conversation that never finishes is a typed i/o failure"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "the call waits out its own deadline rather than failing for some other reason: \
         {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "one deadline covers the whole conversation: {elapsed:?}"
    );
    assert_eq!(
        unsafe { pmws_renew(session) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "the timed-out conversation took the connection with it, so there is nothing left to \
         renew on"
    );

    let market = resolve(session, &market_ref());
    let (status, state, _levels) = read_state(session, market, 8);
    assert_eq!(
        status, PMWS_STATUS_OK,
        "the mapping outlives the connection exactly as it outlives the lease"
    );
    assert_eq!(state.level_count, 2, "the published book is still there");

    unsafe { pmws_close(session) };
    let conversation = server.join().expect("the scripted daemon finishes");
    assert!(
        conversation.closed_by_peer,
        "the daemon sees the close, which releases every lease the session held"
    );
    assert_eq!(
        requests(&conversation.asked),
        vec![
            ControlRequest::Attach {
                market: SLUG.to_owned()
            },
            ControlRequest::Release {
                market: SLUG.to_owned()
            },
        ],
        "nothing is written to a connection this side has given up on"
    );
    let _removed = std::fs::remove_file(socket.as_path());
    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// The rollback of a foreign attachment runs under the attach conversation's own deadline
/// rather than opening a second one, so a lease costs one bound in the worst case and not
/// two.
///
/// The attach is answered three seconds in, leaving about two: a rollback that never answers
/// therefore ends the call at roughly five seconds, where a fresh deadline would end it at
/// about eight. The gap is wide enough that the bounds below do not have to be tight.
#[test]
fn ffi_lease_rollback_runs_under_the_attach_conversations_own_deadline() {
    let (path, _region, mut writer) = build_segment("rollback-budget", 4, 4, 8, 8);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let doorbell = declared_doorbell(&writer);

    let script = vec![
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-0.seg", doorbell),
        }),
        Step::attaching(&ControlResponse::Attached {
            attachment: scripted_attachment("shard-1.seg", doorbell),
        })
        .after(Duration::from_secs(3)),
        Step::stalling(),
    ];
    let (socket, server) = scripted_daemon("rollback-budget", path.as_path(), script);
    let session = connect_scripted(socket.as_path(), SLUG);

    let started = Instant::now();
    let status = lease(session, "elsewhere");
    let elapsed = started.elapsed();
    assert_eq!(
        status, PMWS_STATUS_IO,
        "a rollback that ran out of budget is an unconfirmed one"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "the rollback spent what the attach left of the budget rather than giving up at \
         once: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(6_500),
        "one budget covers the attach and the rollback that undoes it, not one each: \
         {elapsed:?}"
    );
    assert_eq!(
        unsafe { pmws_renew(session) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "an exhausted budget is a failed conversation, and a failed conversation takes the \
         connection"
    );

    unsafe { pmws_close(session) };
    let conversation = server.join().expect("the scripted daemon finishes");
    assert!(conversation.closed_by_peer, "the daemon sees the close");
    assert_eq!(
        requests(&conversation.asked),
        vec![
            ControlRequest::Attach {
                market: SLUG.to_owned()
            },
            ControlRequest::Attach {
                market: "elsewhere".to_owned()
            },
            ControlRequest::Release {
                market: "elsewhere".to_owned()
            },
        ],
        "the rollback is sent inside the budget it shares, and nothing follows it"
    );
    let _removed = std::fs::remove_file(socket.as_path());
    let _removed = std::fs::remove_file(path.as_path());
    let _removed = std::fs::remove_file(doorbell_page_path(path.as_path()));
}

/// Leasing and releasing need what renewing needs — a session that came through a daemon's
/// control socket — and refuse the argument shapes every other entry point of this ABI
/// refuses.
#[test]
fn ffi_lease_and_release_need_a_session_that_holds_a_lease() {
    let (path, _region, mut writer) = build_segment("lease-args", 1, 1, 4, 4);
    let _handle = writer.install(&market_ref()).unwrap();
    let session = open_session(&path);
    let bad_utf8: &[u8] = &[0xff, 0xfe];

    assert_eq!(
        unsafe { pmws_lease(ptr::null(), SLUG.as_ptr(), SLUG.len()) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a null session is a caller error"
    );
    assert_eq!(
        unsafe { pmws_release(ptr::null(), SLUG.as_ptr(), SLUG.len()) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a null session is a caller error"
    );
    assert_eq!(
        lease(session, SLUG),
        PMWS_STATUS_INVALID_ARGUMENT,
        "a session opened from a path has no control connection to take a lease on"
    );
    assert_eq!(
        release(session, SLUG),
        PMWS_STATUS_INVALID_ARGUMENT,
        "a session opened from a path holds no lease to give back"
    );
    assert_eq!(
        unsafe { pmws_lease(session, ptr::null(), 0) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a market is required"
    );
    assert_eq!(
        unsafe { pmws_release(session, SLUG.as_ptr(), 0) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a zero-length market is refused"
    );
    assert_eq!(
        unsafe { pmws_lease(session, bad_utf8.as_ptr(), bad_utf8.len()) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a market that is not utf-8 is refused"
    );
    assert_eq!(
        unsafe { pmws_release(session, bad_utf8.as_ptr(), bad_utf8.len()) },
        PMWS_STATUS_INVALID_ARGUMENT,
        "a market that is not utf-8 is refused"
    );

    unsafe { pmws_close(session) };
    let _removed = std::fs::remove_file(&path);
    let _removed = std::fs::remove_file(doorbell_page_path(&path));
}

#[test]
fn ffi_status_text_is_distinct_non_empty_and_falls_back_for_unknown_codes() {
    let codes = [
        PMWS_STATUS_OK,
        PMWS_STATUS_NONE,
        PMWS_STATUS_INVALID_ARGUMENT,
        PMWS_STATUS_IO,
        PMWS_STATUS_SEGMENT_INCOMPATIBLE,
        PMWS_STATUS_MARKET_NOT_FOUND,
        PMWS_STATUS_NO_PUBLISHED_STATE,
        PMWS_STATUS_CONTENDED,
        PMWS_STATUS_WRITER_STALLED,
        PMWS_STATUS_MALFORMED_RECORD,
        PMWS_STATUS_BUFFER_TOO_SMALL,
        PMWS_STATUS_CONTINUITY_LOST,
        PMWS_STATUS_NOT_ATTACHED,
        PMWS_STATUS_INTERNAL,
        PMWS_STATUS_ATTACH_REFUSED,
        PMWS_STATUS_ATTACH_INCOMPLETE,
        PMWS_STATUS_DOORBELL_UNAVAILABLE,
        PMWS_STATUS_FOREIGN_SEGMENT,
    ];
    let mut seen = std::collections::HashSet::new();
    for code in codes {
        let text = unsafe { CStr::from_ptr(pmws_status_text(code)) }
            .to_str()
            .unwrap();
        assert!(!text.is_empty());
        assert!(
            seen.insert(text.to_owned()),
            "status {code} reused text {text:?}"
        );
    }
    let unknown = unsafe { CStr::from_ptr(pmws_status_text(9_999)) }
        .to_str()
        .unwrap();
    assert_eq!(unknown, "unknown status");

    assert_eq!(pmws_ffi_version(), 8);
    assert_eq!(pmws_abi_version(), pm_ws::ABI_VERSION);
}

const RESOLUTION_DATE: &str = "2026-09-01T12:05:00Z";

fn resolution(winner: &str, index: u32, native_label: &str) -> MarketResolution {
    MarketResolution::new(
        ResolutionObservation::new(
            provenance(),
            NativeOutcome::venue_defined(winner).unwrap(),
            NativeLabel::new(native_label).unwrap(),
            DeliveryPath::MarketFeed,
        )
        .unwrap(),
        index,
        SourceTimestamp::new(RESOLUTION_DATE).unwrap(),
    )
}

fn text_of(bytes: &[u8], len: u32) -> &str {
    std::str::from_utf8(&bytes[..len as usize]).unwrap()
}

/// A market resolution crosses the C ABI as delivery kind 3, with every venue-native field
/// exact and every mutation-only field zeroed.
///
/// A caller reading `side`, `price` or `native_family` off a resolution must find nothing
/// rather than stale bytes from the mutation before it: the delivery kind is the only thing
/// that says which fields mean anything.
#[test]
fn ffi_delivers_a_market_resolution_with_its_venue_native_fields() {
    let (path, _region, mut writer) = build_segment("resolution", 2, 2, 16, 64);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let market = resolve(session, &market_ref());
    let (status, _state, _levels) = attach(session, market, 16);
    assert_eq!(status, PMWS_STATUS_OK);

    let ordered_after = book.revision();
    let cursor = book.note_stream_event().unwrap();
    writer
        .publish_resolution(
            handle,
            ordered_after,
            &cursor,
            &resolution("Yes", 4, "clob"),
            0,
        )
        .unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(event.delivery_kind, 3);
    assert_eq!(event.cursor_epoch, cursor.epoch());
    assert_eq!(event.cursor_position, cursor.position());
    assert_eq!(event.book_revision, ordered_after);
    assert_eq!(event.origin, 1, "SourceReported");
    assert_eq!(event.derivation, 0);
    assert_eq!(event.representation, 1, "VenueNative");
    assert_eq!(event.daemon_generation, 3);
    assert_eq!(event.subscription_generation, 7);
    assert!(event.commit_time != 0);
    assert_eq!(event.winning_index, 4);
    assert_eq!(event.delivery_path, 1, "MarketFeed");
    assert_eq!(
        text_of(&event.winning_outcome, event.winning_outcome_len),
        "Yes"
    );
    assert_eq!(text_of(&event.market_type, event.market_type_len), "clob");
    assert_eq!(
        text_of(&event.resolution_date, event.resolution_date_len),
        RESOLUTION_DATE
    );
    assert_eq!(event.reserved2, 0);
    assert_eq!(event.reserved3, 0);
    assert_eq!(event.arrival_time, 0, "this test stamps no arrival");
    assert_eq!(event.continuity_reason, 0);
    assert_eq!(event.missed, 0);
    assert_eq!(
        (event.side, event.old_present, event.new_present),
        (0, 0, 0)
    );
    assert_eq!(event.native_family_len, 0);
    assert_eq!(event.native_family, [0_u8; 64]);
    for decimal in [event.price, event.old_quantity, event.new_quantity] {
        assert_eq!(
            (
                decimal.coefficient_low,
                decimal.coefficient_high,
                decimal.scale
            ),
            (0, 0, 0)
        );
    }

    let commit = book.apply_source_delta(&delta_at(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let (status, event) = next_event(session, market);
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(
        event.delivery_kind, 1,
        "the book update after a resolution is delivered exactly as before"
    );
    assert_eq!(event.cursor_position, cursor.position() + 1);
    assert_eq!(
        text_of(&event.winning_outcome, event.winning_outcome_len),
        "",
        "a mutation carries no resolution text"
    );
    assert_eq!(event.winning_index, 0);
    assert_eq!(event.delivery_path, 0);

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}

/// A wait with no publish behind it ends at its own deadline, reporting
/// [`PMWS_STATUS_NONE`] and the generation it started with.
#[test]
fn ffi_wait_times_out_with_no_publish() {
    let (path, _region, mut writer) = build_segment("wait-timeout", 2, 2, 16, 16);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let before = unsafe { pmws_publication_generation(session) };
    let mut generation = u64::MAX;
    let started = Instant::now();
    let status = unsafe { pmws_wait(session, before, 0, 50, &mut generation) };
    assert_eq!(status, PMWS_STATUS_NONE);
    assert_eq!(generation, before);
    assert!(started.elapsed() >= Duration::from_millis(40));

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// A parked `pmws_wait`, on a real file-backed segment's own doorbell placement, wakes on a
/// publish from another thread and reports the fresh generation.
///
/// As with the equivalent writer-side and reader-side doorbell tests, the assertion is "not
/// a timeout" rather than "woken by the syscall specifically": `pmws_wait`'s own pre-park
/// recheck of the generation closes the lost-wake window, so a publish landing before the
/// spawned thread reaches the wait syscall is caught there instead, and both are the
/// property under test. The 10-second deadline is a safety net this test never actually
/// waits out.
#[test]
fn ffi_wait_is_woken_by_a_publish_from_a_thread() {
    let (path, _region, mut writer) = build_segment("wait-wake", 2, 2, 16, 16);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let before = unsafe { pmws_publication_generation(session) };
    // A raw session pointer is not `Send`; the round trip through `usize` is the standard
    // way to move one into a scoped thread that only ever touches it sequentially with this
    // one, matching `PmwsSession`'s own documented single-thread-at-a-time contract.
    let session_addr = session as usize;

    let (status, generation) = std::thread::scope(|scope| {
        let parked = scope.spawn(move || {
            let session = session_addr as *const PmwsSession;
            let mut generation = 0_u64;
            let status = unsafe { pmws_wait(session, before, 0, 10_000, &mut generation) };
            (status, generation)
        });
        std::thread::sleep(Duration::from_millis(50));
        writer.publish(handle, &book.publish(), 0).unwrap();
        parked.join().expect("the parked thread joins")
    });

    assert_eq!(status, PMWS_STATUS_OK);
    assert!(generation > before);
    assert_eq!(generation, unsafe { pmws_publication_generation(session) });

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// `pmws_next_dirty`'s full lifecycle: a delivered entry naming the right directory index
/// and revision, idle once caught up, the declared full-rescan signal once the writer laps
/// the ring past this session's cursor, and ordinary polling resumed right after — never a
/// second, sticky rescan.
#[test]
fn ffi_next_dirty_lifecycle_including_rescan() {
    let (path, _region, mut writer) = build_segment("next-dirty", 2, 2, 16, 16);
    let alpha = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();

    let session = open_session(&path);
    let mut directory_index = u32::MAX;
    let mut book_revision = u64::MAX;

    // The session's dirty cursor is created lazily, at the ring's current head, on this
    // first call — which is empty, since nothing has published yet.
    let status = unsafe { pmws_next_dirty(session, &mut directory_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_NONE, "the ring is still empty");

    writer.publish(alpha, &book.publish(), 0).unwrap();
    let status = unsafe { pmws_next_dirty(session, &mut directory_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(directory_index, alpha.entry_index());
    assert_eq!(book_revision, book.revision());

    let status = unsafe { pmws_next_dirty(session, &mut directory_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_NONE, "nothing further has changed yet");

    // The segment's dirty ring is created at capacity 16 (`build_segment`'s fixed depth);
    // 17 more publications lap it past the position this session's cursor now expects (1).
    for _ in 0..17 {
        writer.publish(alpha, &book.publish(), 0).unwrap();
    }
    let status = unsafe { pmws_next_dirty(session, &mut directory_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_CONTINUITY_LOST);

    // Not sticky: the session's cursor already rebased, so the very next call resumes
    // ordinary polling rather than repeating the signal.
    let status = unsafe { pmws_next_dirty(session, &mut directory_index, &mut book_revision) };
    assert!(
        matches!(status, PMWS_STATUS_OK | PMWS_STATUS_NONE),
        "a rescan must not repeat once the cursor has rebased, got status {status}"
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// `pmws_market_directory_index` is the bridge between the two index namespaces: it turns a
/// session-local `market` into the segment directory index `pmws_next_dirty` delivers.
///
/// Two markets are installed and only the second is resolved, so the session-local index (0)
/// and the directory index (1) differ — on a one-market segment they coincide and the
/// mapping would prove nothing.
#[test]
fn ffi_market_directory_index_maps_a_session_index_onto_a_delivered_dirty_entry() {
    let (path, _region, mut writer) = build_segment("directory-index", 2, 2, 16, 16);
    let _alpha = writer.install(&market_ref()).unwrap();
    let beta = writer.install(&second_market_ref()).unwrap();
    assert_eq!(beta.entry_index(), 1);

    let session = open_session(&path);
    let market = resolve(session, &second_market_ref());
    assert_eq!(
        market, 0,
        "the first identity this session resolves is index 0"
    );

    let mut directory_index = u32::MAX;
    let status = unsafe { pmws_market_directory_index(session, market, &mut directory_index) };
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(directory_index, beta.entry_index());
    assert_ne!(
        directory_index, market,
        "the test would prove nothing if the two namespaces coincided here"
    );

    // The session's dirty cursor is created lazily at the ring's current head on the first
    // call, so it is established here — while the ring is empty — rather than after the
    // publication it is meant to deliver.
    let mut delivered_index = u32::MAX;
    let mut book_revision = u64::MAX;
    let status = unsafe { pmws_next_dirty(session, &mut delivered_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_NONE, "the ring is still empty");

    let book = OrderBook::new(second_market_ref());
    writer.publish(beta, &book.publish(), 0).unwrap();

    let status = unsafe { pmws_next_dirty(session, &mut delivered_index, &mut book_revision) };
    assert_eq!(status, PMWS_STATUS_OK);
    assert_eq!(
        delivered_index, directory_index,
        "a consumer must be able to match a delivered dirty entry to a market it resolved"
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// A `market` this session never resolved, and a null out-pointer, are both refused.
#[test]
fn ffi_market_directory_index_refuses_an_unresolved_market_and_a_null_out() {
    let (path, _region, mut writer) = build_segment("directory-index-bad", 2, 2, 16, 16);
    let _handle = writer.install(&market_ref()).unwrap();
    let session = open_session(&path);
    let market = resolve(session, &market_ref());

    let mut directory_index = u32::MAX;
    assert_eq!(
        unsafe { pmws_market_directory_index(session, market + 1, &mut directory_index) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_market_directory_index(session, market, ptr::null_mut()) },
        PMWS_STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { pmws_market_directory_index(ptr::null(), market, &mut directory_index) },
        PMWS_STATUS_INVALID_ARGUMENT
    );

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// A spin budget wider than [`PMWS_MAX_SPIN_MICROS`] is refused before any spinning happens.
///
/// The elapsed-time assertion is the point: a `spin_micros` a caller's own narrowing produced
/// — `-1` reaching an unsigned parameter as `u32::MAX` — must cost nothing, not seventy-one
/// minutes of a burned core. A budget at the ceiling is not exercised here for the same
/// reason: honouring it would spin for ten seconds.
#[test]
fn ffi_wait_refuses_a_spin_budget_above_the_ceiling() {
    let (path, _region, mut writer) = build_segment("spin-ceiling", 2, 2, 16, 16);
    let handle = writer.install(&market_ref()).unwrap();
    let mut book = OrderBook::new(market_ref());
    book.apply_snapshot(&base_snapshot()).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let session = open_session(&path);
    let before = unsafe { pmws_publication_generation(session) };
    let mut generation = u64::MAX;

    let started = Instant::now();
    let status = unsafe { pmws_wait(session, before, u32::MAX, 0, &mut generation) };
    assert_eq!(status, PMWS_STATUS_INVALID_ARGUMENT);
    assert_eq!(generation, u64::MAX, "a refused call writes no generation");
    let status = unsafe {
        pmws_wait(
            session,
            before,
            PMWS_MAX_SPIN_MICROS + 1,
            0,
            &mut generation,
        )
    };
    assert_eq!(status, PMWS_STATUS_INVALID_ARGUMENT);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "an over-ceiling spin budget was honoured rather than refused: took {:?}",
        started.elapsed()
    );

    // A budget inside the ceiling still reaches the wait: this pins the refusal to the range
    // check and not to `spin_micros` being rejected outright.
    let mut generation = u64::MAX;
    let status = unsafe { pmws_wait(session, before, 100, 50, &mut generation) };
    assert_eq!(status, PMWS_STATUS_NONE);
    assert_eq!(generation, before);

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(doorbell_page_path(&path));
    let _ = std::fs::remove_file(&path);
}

/// A page-placement segment whose sibling doorbell page is gone by the time `pmws_open`
/// attaches gives `pmws_wait` nothing to park on. The segment itself opens, reads, and spins
/// normally — only a park fails, and with the distinct
/// [`PMWS_STATUS_DOORBELL_UNAVAILABLE`], never the generic [`PMWS_STATUS_IO`] a real i/o
/// failure reports, so a caller can tell "fall back to spinning" from "something is actually
/// broken".
#[test]
fn ffi_wait_reports_doorbell_unavailable_distinctly_from_io() {
    let (path, _region, writer) = build_page_segment("doorbell-unavailable", 2, 2, 16, 16);
    drop(writer);
    std::fs::remove_file(doorbell_page_path(&path)).expect("the sibling page exists to be removed");

    let session = open_session(&path);
    let before = unsafe { pmws_publication_generation(session) };
    let mut generation = u64::MAX;
    let status = unsafe { pmws_wait(session, before, 0, 50, &mut generation) };
    assert_eq!(
        status, PMWS_STATUS_DOORBELL_UNAVAILABLE,
        "a missing sibling page is a distinct, actionable status, not the generic io failure"
    );
    assert_eq!(generation, u64::MAX, "a failed wait writes no generation");

    // The fault is a property of this session's attachment, not a one-shot capture: every
    // later park attempt answers it identically rather than blocking or lying.
    let mut generation = u64::MAX;
    let status = unsafe { pmws_wait(session, before, 0, 50, &mut generation) };
    assert_eq!(status, PMWS_STATUS_DOORBELL_UNAVAILABLE);
    assert_eq!(generation, u64::MAX);

    unsafe { pmws_close(session) };
    let _ = std::fs::remove_file(&path);
}
