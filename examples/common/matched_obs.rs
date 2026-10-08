//! Shared harness code for the two pm-ws legs of the S8c comparative benchmark
//! (`bench/sdk-harness/README.md`): the canonical-decimal and content-digest rules every leg
//! matches on, the observation log those digests are written to, and the small reporting and
//! lifecycle helpers both legs need.
//!
//! Included by `examples/matched_consumer.rs` and `examples/embedded_live.rs` through a
//! `#[path]` module declaration rather than compiled as an example of its own, which is why
//! it lives in a subdirectory: cargo discovers `examples/*.rs` and `examples/*/main.rs` as
//! targets, and this is neither.
//!
//! Tier-2 bench tooling. Nothing here is on the daemon's path.
#![allow(dead_code)]

use pm_ws::{Level, Side};
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The most observation rows one leg buffers before it starts counting drops, as
/// `bench/sdk-harness/README.md` fixes it.
pub const OBS_ROW_CAP: usize = 4_000_000;

/// A latency at or beyond this is a clock artifact rather than a delivery, and is discarded
/// and counted rather than clamped — the rule `examples/latency_probe.rs` and
/// `examples/shm_latency.rs` already report under.
pub const IMPLAUSIBLE_NANOS: u64 = 1_000_000_000;

/// The fewest kept samples a reported distribution may rest on, mirroring
/// `examples/latency_probe.rs`. A run below it withholds percentiles rather than print a
/// p99.9 that is one outlier dressed up as a distribution.
pub const MIN_REPORTABLE_SAMPLES: usize = 1_000;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Wall-clock nanoseconds since the Unix epoch, the one clock every leg of this harness
/// stamps `t_obs` from.
pub fn now_epoch_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

/// The canonical decimal form of `text`, exactly as `bench/sdk-harness/README.md` defines
/// it, so that a level rendered by any leg in any language hashes to the same bytes.
///
/// The four steps in the specification's own order: expand `e`/`E` notation to plain
/// decimal, strip a leading `+`, strip redundant leading zeros, and — only for a value that
/// carries a `.` — strip trailing zeros and then a trailing `.`.
///
/// Pure string manipulation on purpose. Routing this through the crate's own
/// [`pm_ws::ExactDecimal`] would not implement it: that type preserves the scale it parsed,
/// so `1000000.0` round-trips as `1000000.0` rather than as `1000000`.
///
/// Input that is not a decimal lexeme is returned unchanged, which keeps a malformed value
/// visible in the log rather than silently hashing as something else.
pub fn canonical_decimal(text: &str) -> String {
    let (negative, magnitude) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let Some(expanded) = expand_exponent(magnitude) else {
        return text.to_owned();
    };
    let trimmed = strip_leading_zeros(&expanded);
    let stripped = strip_trailing_zeros(trimmed);
    if negative && stripped != "0" {
        format!("-{stripped}")
    } else {
        stripped.to_owned()
    }
}

/// Rewrites `text` in plain decimal notation, or `None` when it is not a decimal lexeme this
/// harness can canonicalize.
fn expand_exponent(text: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(index) => {
            let raw = &text[index + 1..];
            let raw = raw.strip_prefix('+').unwrap_or(raw);
            (&text[..index], raw.parse::<i32>().ok()?)
        }
        None => (text, 0),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (mantissa, ""),
    };
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits: String = format!("{integer}{fraction}");
    let point = i64::try_from(integer.len()).ok()? + i64::from(exponent);
    let width = i64::try_from(digits.len()).ok()?;
    if point <= 0 {
        let pad = usize::try_from(-point).ok()?;
        Some(format!("0.{}{digits}", "0".repeat(pad)))
    } else if point >= width {
        let pad = usize::try_from(point - width).ok()?;
        Some(format!("{digits}{}", "0".repeat(pad)))
    } else {
        let split = usize::try_from(point).ok()?;
        Some(format!("{}.{}", &digits[..split], &digits[split..]))
    }
}

/// Drops leading zeros that carry no value, keeping the one that precedes a `.` and the one
/// that is the whole value.
fn strip_leading_zeros(text: &str) -> &str {
    let mut rest = text;
    while rest.len() > 1 && rest.starts_with('0') && !rest[1..].starts_with('.') {
        rest = &rest[1..];
    }
    rest
}

/// Drops the trailing zeros of a fractional value, and then the point they were behind.
fn strip_trailing_zeros(text: &str) -> &str {
    if !text.contains('.') {
        return text;
    }
    text.trim_end_matches('0').trim_end_matches('.')
}

/// FNV-1a 64 over `bytes`, with the offset basis and prime
/// `bench/sdk-harness/README.md` pins.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// One digest as the 16 lower-case hexadecimal digits an `.obs` row carries.
pub fn format_digest(digest: u64) -> String {
    format!("{digest:016x}")
}

/// The serialized form a book's levels are digested from: `B` then every bid, `|A` then
/// every ask, each level as `price:qty;` in canonical decimals.
///
/// Levels with zero quantity are excluded, bids are ordered by price descending and asks
/// ascending. The ordering is decided on the exact decimal *value*, and only then rendered:
/// sorting the rendered text would order `0.9` above `0.11` the wrong way round.
pub fn digest_input(levels: &[Level]) -> String {
    let mut bids: Vec<&Level> = levels
        .iter()
        .filter(|level| level.side() == Side::Bid && !is_zero_quantity(level))
        .collect();
    let mut asks: Vec<&Level> = levels
        .iter()
        .filter(|level| level.side() == Side::Ask && !is_zero_quantity(level))
        .collect();
    bids.sort_by(|left, right| right.price().cmp(left.price()));
    asks.sort_by(|left, right| left.price().cmp(right.price()));
    let mut text = String::with_capacity(levels.len() * 16 + 3);
    text.push('B');
    append_levels(&mut text, &bids);
    text.push_str("|A");
    append_levels(&mut text, &asks);
    text
}

fn append_levels(text: &mut String, levels: &[&Level]) {
    for level in levels {
        text.push_str(&canonical_decimal(&level.price().value().canonical()));
        text.push(':');
        text.push_str(&canonical_decimal(&level.quantity().value().canonical()));
        text.push(';');
    }
}

fn is_zero_quantity(level: &Level) -> bool {
    level.quantity().value().coefficient() == 0
}

/// The content digest of one book's levels: FNV-1a 64 over [`digest_input`].
pub fn level_digest(levels: &[Level]) -> u64 {
    fnv1a64(digest_input(levels).as_bytes())
}

/// One market's identity inside an [`ObsLog`], taken once so that recording an observation
/// costs no lookup by name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlugId(u32);

/// One buffered observation. The slug is interned and the digest is kept as the 64-bit value
/// it is, so a run at the row cap costs bytes rather than tens of them per row.
#[derive(Clone, Copy, Debug)]
struct ObsRow {
    slug: SlugId,
    t_obs_ns: u64,
    digest: u64,
    seq: u64,
    revision: u64,
}

/// One leg's observation log, in the `.obs` format `bench/sdk-harness/README.md` fixes.
///
/// Rows are buffered in memory and written once, on exit: writing during the run would put
/// blocking file I/O between two observations. Past [`OBS_ROW_CAP`] rows nothing more is
/// buffered and the overflow is counted as `dropped`; `events_total` counts every
/// observation the leg made, dropped ones included, so `events_total - dropped` is the row
/// count the file carries.
pub struct ObsLog {
    leg: String,
    host: String,
    pin: String,
    size: usize,
    cap: usize,
    slugs: Vec<String>,
    index: HashMap<String, SlugId>,
    next_seq: Vec<u64>,
    rows: Vec<ObsRow>,
    events_total: u64,
    dropped: u64,
}

impl ObsLog {
    /// An empty log for `leg`, labelled with the host label `host`, the pinned market count
    /// `size`, and the revision `pin` the leg was built from.
    pub fn new(leg: &str, host: &str, pin: &str, size: usize, cap: usize) -> Self {
        Self {
            leg: leg.to_owned(),
            host: host.to_owned(),
            pin: pin.to_owned(),
            size,
            cap,
            slugs: Vec::new(),
            index: HashMap::new(),
            next_seq: Vec::new(),
            rows: Vec::new(),
            events_total: 0,
            dropped: 0,
        }
    }

    /// Takes the handle `slug` is recorded under, interning it on first use.
    pub fn intern(&mut self, slug: &str) -> SlugId {
        if let Some(id) = self.index.get(slug) {
            return *id;
        }
        let id = SlugId(u32::try_from(self.slugs.len()).unwrap_or(u32::MAX));
        self.slugs.push(slug.to_owned());
        self.next_seq.push(0);
        self.index.insert(slug.to_owned(), id);
        id
    }

    /// Buffers one observation and returns the per-market sequence number it was given.
    ///
    /// The sequence advances whether or not the row fit, because it names the observation
    /// rather than the row: a matcher pairs on it, and a leg that renumbered after an
    /// overflow would claim to have observed something it did not.
    pub fn record(&mut self, slug: SlugId, t_obs_ns: u64, digest: u64, revision: u64) -> u64 {
        self.events_total = self.events_total.saturating_add(1);
        let slot = usize::try_from(slug.0).unwrap_or(usize::MAX);
        let seq = self.next_seq.get(slot).copied().unwrap_or(0);
        if let Some(next) = self.next_seq.get_mut(slot) {
            *next = next.saturating_add(1);
        }
        if self.rows.len() >= self.cap {
            self.dropped = self.dropped.saturating_add(1);
            return seq;
        }
        self.rows.push(ObsRow {
            slug,
            t_obs_ns,
            digest,
            seq,
            revision,
        });
        seq
    }

    pub fn events_total(&self) -> u64 {
        self.events_total
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn rows(&self) -> usize {
        self.rows.len()
    }

    /// How many distinct markets this log carries an observation for.
    pub fn markets_observed(&self) -> usize {
        self.next_seq.iter().filter(|seq| **seq > 0).count()
    }

    /// Writes the log to `path`, headers and trailers included.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut out = std::io::BufWriter::new(file);
        writeln!(out, "# pmws-obs v1")?;
        writeln!(out, "# leg: {}", self.leg)?;
        writeln!(out, "# host: {}", self.host)?;
        writeln!(out, "# clock: epoch_ns")?;
        writeln!(out, "# size: {}", self.size)?;
        writeln!(out, "# pin: {}", self.pin)?;
        for row in &self.rows {
            let slug = self
                .slugs
                .get(usize::try_from(row.slug.0).unwrap_or(usize::MAX))
                .map_or("?", String::as_str);
            writeln!(
                out,
                "obs {slug} {} {} {} rev={}",
                row.t_obs_ns,
                format_digest(row.digest),
                row.seq,
                row.revision
            )?;
        }
        writeln!(out, "# events_total: {}", self.events_total)?;
        writeln!(out, "# dropped: {}", self.dropped)?;
        out.flush()
    }
}

/// The pinned market set at `path`: one slug per line, blank lines and `#` comments ignored,
/// duplicates collapsed in first-seen order.
///
/// Fails on a file that cannot be read and on a line that is not a venue-native market
/// identifier — a leg that quietly skipped one would measure a smaller set than the runner
/// pinned and report it as the pinned size.
pub fn read_slug_file(path: &Path) -> Result<Vec<String>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut slugs = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (number, line) in text.lines().enumerate() {
        let slug = line.trim();
        if slug.is_empty() || slug.starts_with('#') {
            continue;
        }
        if !pm_ws::limitless::shard::is_market_slug(slug) {
            return Err(format!(
                "{}:{} is not a venue-native market identifier: {slug:?}",
                path.display(),
                number + 1
            ));
        }
        if seen.insert(slug.to_owned()) {
            slugs.push(slug.to_owned());
        }
    }
    if slugs.is_empty() {
        return Err(format!("{} names no market", path.display()));
    }
    Ok(slugs)
}

/// The revision of the checkout this leg was built from, for the log's `# pin:` header, or
/// `unknown` when this is not a git checkout.
///
/// Called once at startup, never on the measured path.
pub fn git_pin() -> String {
    let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
    else {
        return "unknown".to_owned();
    };
    if !output.status.success() {
        return "unknown".to_owned();
    }
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_owned())
        .unwrap_or_else(|_| "unknown".to_owned())
}

/// A flag raised when this process is asked to terminate, so a run flushes its `.obs` file
/// rather than losing it.
///
/// The wait runs on a thread of its own with a runtime of its own, so nothing about signal
/// delivery touches a measurement loop; the loop only ever reads the flag. Both `SIGTERM`
/// and `SIGINT` raise it, which is what makes an interactive run and a runner-killed run end
/// the same way. A process whose signal registration fails simply never raises the flag and
/// ends on its own `--seconds` deadline.
pub fn termination_flag() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let raised = Arc::clone(&flag);
    let spawned = std::thread::Builder::new()
        .name("pmws-termination".to_owned())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                use tokio::signal::unix::{SignalKind, signal};
                let (Ok(mut terminate), Ok(mut interrupt)) = (
                    signal(SignalKind::terminate()),
                    signal(SignalKind::interrupt()),
                ) else {
                    return;
                };
                tokio::select! {
                    _ = terminate.recv() => {}
                    _ = interrupt.recv() => {}
                }
                raised.store(true, Ordering::Relaxed);
            });
        });
    if let Err(error) = spawned {
        eprintln!("note: no termination handler ({error}); the run ends on --seconds alone");
    }
    flag
}

/// The `permille`-th value of a sorted nanosecond sample set, by nearest rank; 0 for an
/// empty set. Identical to `examples/latency_probe.rs`'s own `quantile`.
pub fn quantile(sorted: &[u64], permille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * permille).div_ceil(1000).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)]
}

pub fn format_micros(nanos: u64) -> String {
    format!("{:.3}", nanos as f64 / 1000.0)
}

/// Prints one distribution under `prefix`, in the shape `examples/latency_probe.rs` prints
/// its own: the same nearest-rank quantiles, the same [`MIN_REPORTABLE_SAMPLES`] floor, and
/// percentiles withheld rather than printed below it.
pub fn print_distribution(prefix: &str, kept: &[u64], discarded: u64) {
    println!("{prefix}_samples_kept: {}", kept.len());
    if kept.len() < MIN_REPORTABLE_SAMPLES {
        println!(
            "{prefix}_percentiles: withheld, only {} kept samples (floor is \
             {MIN_REPORTABLE_SAMPLES})",
            kept.len()
        );
    } else {
        let mut sorted = kept.to_vec();
        sorted.sort_unstable();
        println!("{prefix}_p50_us: {}", format_micros(quantile(&sorted, 500)));
        println!("{prefix}_p95_us: {}", format_micros(quantile(&sorted, 950)));
        println!("{prefix}_p99_us: {}", format_micros(quantile(&sorted, 990)));
        println!(
            "{prefix}_p99.9_us: {}",
            format_micros(quantile(&sorted, 999))
        );
        println!(
            "{prefix}_max_us: {}",
            format_micros(sorted.last().copied().unwrap_or(0))
        );
    }
    println!("{prefix}_samples_discarded: {discarded}");
}

/// The most seconds one run may be asked to last, mirroring `examples/latency_probe.rs`.
pub const MAX_SECONDS: u64 = 86_400;

/// Parses `--seconds` under the shared ceiling.
pub fn parse_seconds(value: &str) -> Result<u64, String> {
    let seconds: u64 = value
        .parse()
        .map_err(|_| "--seconds takes a positive integer".to_owned())?;
    if seconds == 0 || seconds > MAX_SECONDS {
        return Err(format!("--seconds must be between 1 and {MAX_SECONDS}"));
    }
    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pm_ws::{DecimalGrammar, Price, Quantity};

    fn grammar() -> DecimalGrammar {
        DecimalGrammar::new(18, 30, true, false).expect("bench decimal grammar is valid")
    }

    fn level(side: Side, price: &str, quantity: &str) -> Level {
        Level::new(
            side,
            Price::parse(price, grammar()).expect("price parses"),
            Quantity::parse(quantity, grammar()).expect("quantity parses"),
        )
    }

    /// Every vector `bench/sdk-harness/README.md` states, each on its own, because the two
    /// that catch an over-eager strip are `100` (no point, no trailing-zero strip) and `0.0`
    /// (a point whose whole fraction goes).
    #[test]
    fn canonical_decimal_matches_every_specified_vector() {
        assert_eq!(canonical_decimal("0.530"), "0.53");
        assert_eq!(canonical_decimal("1000000.0"), "1000000");
        assert_eq!(canonical_decimal("100"), "100");
        assert_eq!(canonical_decimal("0.5"), "0.5");
        assert_eq!(canonical_decimal("1e-7"), "0.0000001");
        assert_eq!(canonical_decimal("0.0"), "0");
    }

    #[test]
    fn canonical_decimal_strips_redundant_leading_zeros_and_a_leading_plus() {
        assert_eq!(canonical_decimal("007"), "7");
        assert_eq!(canonical_decimal("0"), "0");
        assert_eq!(canonical_decimal("0000"), "0");
        assert_eq!(canonical_decimal("+0.53"), "0.53");
        assert_eq!(canonical_decimal("00.530"), "0.53");
    }

    #[test]
    fn canonical_decimal_expands_exponent_notation_in_both_directions() {
        assert_eq!(canonical_decimal("1E-7"), "0.0000001");
        assert_eq!(canonical_decimal("1e7"), "10000000");
        assert_eq!(canonical_decimal("1.25e2"), "125");
        assert_eq!(canonical_decimal("1.25e+2"), "125");
        assert_eq!(canonical_decimal("12.5e-3"), "0.0125");
        assert_eq!(canonical_decimal("-1.50e1"), "-15");
    }

    /// A lexeme this harness cannot canonicalize stays visible rather than hashing as
    /// something else.
    #[test]
    fn canonical_decimal_returns_a_non_decimal_unchanged() {
        assert_eq!(canonical_decimal(""), "");
        assert_eq!(canonical_decimal("NaN"), "NaN");
        assert_eq!(canonical_decimal("1e"), "1e");
        assert_eq!(canonical_decimal("."), ".");
    }

    /// The offset basis alone, which is what an empty input hashes to under FNV-1a.
    #[test]
    fn fnv1a64_of_nothing_is_the_offset_basis() {
        assert_eq!(format_digest(fnv1a64(b"")), "cbf29ce484222325");
    }

    /// The golden was computed with an independent FNV-1a implementation over the exact
    /// bytes the assertion below names, so it proves the digest rather than restating it.
    #[test]
    fn a_books_digest_matches_the_independently_computed_golden() {
        let levels = vec![
            level(Side::Bid, "0.39", "50.0"),
            level(Side::Bid, "0.400", "0"),
            level(Side::Bid, "0.530", "100"),
            level(Side::Ask, "0.61", "1000000.0"),
            level(Side::Ask, "0.60", "200"),
        ];
        assert_eq!(
            digest_input(&levels),
            "B0.53:100;0.39:50;|A0.6:200;0.61:1000000;",
            "zero-quantity levels are excluded, bids descend and asks ascend"
        );
        assert_eq!(format_digest(level_digest(&levels)), "b1fb49c60cc32156");
    }

    /// An empty book still digests, and to the same value from either leg.
    #[test]
    fn an_empty_book_digests_to_the_empty_serialization() {
        assert_eq!(digest_input(&[]), "B|A");
        assert_eq!(format_digest(level_digest(&[])), "16893f19b12e316e");
    }

    /// Level order on the wire never reaches the digest: the same book presented in any
    /// order hashes the same, which is what lets two legs that sort differently match.
    #[test]
    fn digest_ignores_the_order_levels_arrive_in() {
        let ascending = vec![
            level(Side::Bid, "0.11", "1"),
            level(Side::Bid, "0.9", "2"),
            level(Side::Ask, "0.91", "3"),
        ];
        let descending = vec![
            level(Side::Ask, "0.91", "3"),
            level(Side::Bid, "0.9", "2"),
            level(Side::Bid, "0.11", "1"),
        ];
        assert_eq!(digest_input(&ascending), "B0.9:2;0.11:1;|A0.91:3;");
        assert_eq!(level_digest(&ascending), level_digest(&descending));
    }

    /// Two renderings of one price are one price: the ABI preserves the scale it was given,
    /// so a leg that skipped canonicalization would hash `0.60` and `0.6` apart.
    #[test]
    fn digest_is_blind_to_the_scale_a_level_was_reported_with() {
        let wide = vec![level(Side::Bid, "0.600", "100.00")];
        let narrow = vec![level(Side::Bid, "0.6", "100")];
        assert_eq!(level_digest(&wide), level_digest(&narrow));
    }

    #[test]
    fn a_log_numbers_each_market_from_zero_and_counts_overflow_as_dropped() {
        let mut log = ObsLog::new("rust-shm", "host", "abc123", 2, 3);
        let left = log.intern("alpha");
        let right = log.intern("beta");
        assert_eq!(log.record(left, 10, 1, 100), 0);
        assert_eq!(log.record(right, 11, 2, 200), 0);
        assert_eq!(log.record(left, 12, 3, 101), 1);
        assert_eq!(log.record(left, 13, 4, 102), 2);
        assert_eq!(log.rows(), 3);
        assert_eq!(log.dropped(), 1);
        assert_eq!(log.events_total(), 4);
        assert_eq!(log.markets_observed(), 2);
    }

    #[test]
    fn a_written_log_carries_the_specified_headers_rows_and_trailers() {
        let directory = std::env::temp_dir().join(format!(
            "pmws-obs-{}-{}",
            std::process::id(),
            now_epoch_nanos()
        ));
        std::fs::create_dir_all(&directory).expect("a temporary directory");
        let path = directory.join("leg.obs");
        let mut log = ObsLog::new("rust-embedded", "m1", "deadbeef", 1, OBS_ROW_CAP);
        let slug = log.intern("btc-up-or-down-5-min-1");
        let _seq = log.record(slug, 1_700_000_000_000_000_000, 0x0123_4567_89ab_cdef, 7);
        log.write(&path).expect("the log writes");
        let text = std::fs::read_to_string(&path).expect("the log reads back");
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("# pmws-obs v1"));
        assert_eq!(lines.next(), Some("# leg: rust-embedded"));
        assert_eq!(lines.next(), Some("# host: m1"));
        assert_eq!(lines.next(), Some("# clock: epoch_ns"));
        assert_eq!(lines.next(), Some("# size: 1"));
        assert_eq!(lines.next(), Some("# pin: deadbeef"));
        assert_eq!(
            lines.next(),
            Some("obs btc-up-or-down-5-min-1 1700000000000000000 0123456789abcdef 0 rev=7")
        );
        assert_eq!(lines.next(), Some("# events_total: 1"));
        assert_eq!(lines.next(), Some("# dropped: 0"));
        assert_eq!(lines.next(), None);
        let _removed = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_slug_file_ignores_blanks_and_comments_and_collapses_duplicates() {
        let directory = std::env::temp_dir().join(format!(
            "pmws-slugs-{}-{}",
            std::process::id(),
            now_epoch_nanos()
        ));
        std::fs::create_dir_all(&directory).expect("a temporary directory");
        let path = directory.join("shard-00.txt");
        std::fs::write(
            &path,
            "# shard 0\n\nbeta\nalpha\n  beta  \n\n# trailing comment\n",
        )
        .expect("the slug file writes");
        let slugs = read_slug_file(&path).expect("the slug file parses");
        assert_eq!(slugs, vec!["beta".to_owned(), "alpha".to_owned()]);

        let too_long = "x".repeat(2048);
        std::fs::write(&path, format!("alpha\n{too_long}\n")).expect("the slug file writes");
        let error = read_slug_file(&path).expect_err("an invalid identifier is refused");
        assert!(
            error.contains("not a venue-native market identifier"),
            "an identifier the venue could not have issued was accepted"
        );

        std::fs::write(&path, "# nothing\n").expect("the slug file writes");
        let error = read_slug_file(&path).expect_err("an empty set is refused");
        assert!(error.contains("names no market"), "{error}");
        let _removed = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn quantile_matches_nearest_rank_and_answers_zero_for_nothing() {
        let sorted = [10_u64, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        assert_eq!(quantile(&sorted, 500), 50);
        assert_eq!(quantile(&sorted, 990), 100);
        assert_eq!(quantile(&[], 500), 0);
    }

    #[test]
    fn parse_seconds_rejects_zero_and_the_ceiling() {
        assert_eq!(parse_seconds("600"), Ok(600));
        assert!(parse_seconds("0").is_err());
        assert!(parse_seconds(&(MAX_SECONDS + 1).to_string()).is_err());
        assert!(parse_seconds("later").is_err());
    }
}
