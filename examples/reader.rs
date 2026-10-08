//! Reads one market's BBO out of a published shared-memory segment, in a separate process
//! from the writer.
//!
//! Usage: `reader --segment <path> --market <native-key> [--venue <venue>] [--kind <kind>]
//! [--seconds <n>]`
//!
//! The segment is mapped read-only, its header validated before anything else is touched,
//! and the market resolved by its venue-native identity. One line is printed for every
//! change of `book_revision`; the loop polls `publication_generation` with a short sleep,
//! which is enough to prove the layout. This example polls; it does not park on a
//! reactive wake-up channel that would let a consumer sleep until the writer says newer
//! data exists.

use pm_ws::{
    Level, MarketRef, NativeIdentifierKind, NativeMarketKey, ReadFault, SegmentReader,
    SegmentRegion, Venue,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_micros(200);

fn main() {
    if let Err(message) = run() {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut segment: Option<PathBuf> = None;
    let mut key: Option<String> = None;
    let mut venue = "limitless".to_owned();
    let mut kind = "slug".to_owned();
    let mut seconds = 10_u64;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--segment" => segment = Some(PathBuf::from(value()?)),
            "--market" => key = Some(value()?),
            "--venue" => venue = value()?,
            "--kind" => kind = value()?,
            "--seconds" => {
                seconds = value()?
                    .parse()
                    .map_err(|_| "--seconds must be a whole number".to_owned())?;
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let segment = segment.ok_or("--segment is required")?;
    let key = key.ok_or("--market is required")?;
    let market = MarketRef::new(
        Venue::new(venue).map_err(|error| error.to_string())?,
        NativeMarketKey::new(
            NativeIdentifierKind::new(kind, 128).map_err(|error| error.to_string())?,
            key,
        )
        .map_err(|error| error.to_string())?,
    );

    let region = Arc::new(
        SegmentRegion::open_file(&segment).map_err(|error| format!("map {segment:?}: {error}"))?,
    );
    let reader = SegmentReader::attach(region).map_err(|error| format!("attach: {error}"))?;
    println!(
        "attached instance={:#034x} generation={} markets={}",
        reader.geometry().daemon_instance_id(),
        reader.geometry().segment_generation(),
        reader.geometry().layout().directory_capacity()
    );
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let handle = loop {
        if let Some(handle) = reader.resolve(&market) {
            break handle;
        }
        if Instant::now() >= deadline {
            return Err(format!("market {market:?} is not installed in the segment"));
        }
        std::thread::sleep(POLL_INTERVAL);
    };

    let mut last_generation: Option<u64> = None;
    let mut last_revision = None;
    while Instant::now() < deadline {
        let generation = reader.publication_generation();
        if last_generation == Some(generation) {
            std::thread::sleep(POLL_INTERVAL);
            continue;
        }
        last_generation = Some(generation);
        match reader.read(handle) {
            Ok(book) => {
                if last_revision != Some(book.revision()) {
                    last_revision = Some(book.revision());
                    println!(
                        "bbo revision={} authority={:?} best_bid={} best_ask={}",
                        book.revision(),
                        book.authority(),
                        format_level(book.best_bid()),
                        format_level(book.best_ask())
                    );
                }
            }
            Err(
                ReadFault::NoPublishedState
                | ReadFault::Contended { .. }
                | ReadFault::WriterStalled { .. },
            ) => {
                last_generation = None;
            }
            Err(other) => return Err(format!("read: {other:?}")),
        }
    }
    Ok(())
}

fn format_level(level: Option<&Level>) -> String {
    level.map_or_else(
        || "-".to_owned(),
        |level| format!("{}@{}", level.price().value(), level.quantity().value()),
    )
}
