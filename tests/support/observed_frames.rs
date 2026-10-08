//! Wire frames captured from live `wss://ws.limitless.exchange` sessions, shared between
//! `tests/wire_observed.rs`'s decode-classification contracts and any other integration test
//! that needs to replay the exact bytes a real connection carried rather than a hand-written
//! approximation of them.
//!
//! See `tests/wire_observed.rs` for the full provenance note and `docs/limitless.md` for the
//! observations these frames back.

/// The observed `marketResolved` event, delivered to the resolved market's own room. Sent by
/// the venue three times, byte-identically, within 200 ms; the fixture keeps one copy.
pub const OBSERVED_MARKET_RESOLVED_OWN_ROOM: &str = r#"42/markets,["marketResolved",{"slug":"btc-up-or-down-5-min-1788267900","type":"CLOB","winningOutcome":"NO","winningIndex":1,"resolutionDate":"2026-09-01T13:11:02.813Z"}]"#;

/// The last `orderbookUpdate` the venue sent, for the same market and session, before the
/// observed resolution above.
pub const OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"btc-up-or-down-5-min-1788267900","orderbook":{"bids":[{"price":0.002,"size":50000000,"side":"BUY"}],"asks":[{"price":0.998,"size":50000000,"side":"SELL"}],"tokenId":"83416341894274737086755695271958877285974747994652828356442717729857948830534","adjustedMidpoint":0.5,"midpoint":0.5,"maxSpread":0.065,"minSize":50000000},"version":504852,"timestamp":"2026-09-01T13:09:45.277Z"}]"#;

/// The market slug both observed frames above name.
pub const OBSERVED_RESOLUTION_MARKET_SLUG: &str = "btc-up-or-down-5-min-1788267900";
