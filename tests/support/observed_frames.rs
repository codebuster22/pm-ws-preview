//! Wire frames captured from live `wss://ws.limitless.exchange` sessions: a 60 s session on
//! 2026-08-31 subscribed to the CLOB market `eth-up-or-down-daily-1788105600`, and a 420 s
//! session on 2026-09-01 subscribed to `btc-up-or-down-5-min-1788267900` that spanned the
//! market's resolution. Session identifiers are redacted; every other byte, including the
//! venue-native `tokenId`, is reproduced unchanged. These are the only venue bytes the
//! repository retains, kept solely as decoder regression inputs shared by the integration
//! tests; `docs/limitless.md` records the observations they back.

/// The observed `marketResolved` event, delivered to the resolved market's own room. Sent by
/// the venue three times, byte-identically, within 200 ms; the fixture keeps one copy.
pub const OBSERVED_MARKET_RESOLVED_OWN_ROOM: &str = r#"42/markets,["marketResolved",{"slug":"btc-up-or-down-5-min-1788267900","type":"CLOB","winningOutcome":"NO","winningIndex":1,"resolutionDate":"2026-09-01T13:11:02.813Z"}]"#;

/// The last `orderbookUpdate` the venue sent, for the same market and session, before the
/// observed resolution above.
pub const OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"btc-up-or-down-5-min-1788267900","orderbook":{"bids":[{"price":0.002,"size":50000000,"side":"BUY"}],"asks":[{"price":0.998,"size":50000000,"side":"SELL"}],"tokenId":"83416341894274737086755695271958877285974747994652828356442717729857948830534","adjustedMidpoint":0.5,"midpoint":0.5,"maxSpread":0.065,"minSize":50000000},"version":504852,"timestamp":"2026-09-01T13:09:45.277Z"}]"#;

/// The market slug both observed frames above name.
pub const OBSERVED_RESOLUTION_MARKET_SLUG: &str = "btc-up-or-down-5-min-1788267900";

/// The Engine.IO open packet from the 2026-08-31 session; the session id is redacted.
pub const OBSERVED_ENGINEIO_OPEN: &str = r#"0{"sid":"REPLAY-SESSION-0002","upgrades":[],"pingInterval":25000,"pingTimeout":60000,"maxPayload":1000000}"#;

/// The `/markets` namespace connect acknowledgement from the same session; the namespace id is redacted.
pub const OBSERVED_NAMESPACE_CONNECT_ACK: &str = r#"40/markets,{"sid":"REPLAY-NAMESPACE-0002"}"#;

/// The `system` notice the venue sends once the connection is registered.
pub const OBSERVED_SYSTEM_REGISTERED: &str =
    r#"42/markets,["system",{"message":"Successfully registered connection"}]"#;

/// The `system` acknowledgement naming the subscribed market set.
pub const OBSERVED_SYSTEM_SUBSCRIBED: &str = r#"42/markets,["system",{"message":"Successfully subscribed to market price updates","markets":["eth-up-or-down-daily-1788105600"]}]"#;

/// The first `orderbookUpdate` after subscribing on 2026-08-31: a full ladder for `eth-up-or-down-daily-1788105600`, with the venue-native `tokenId` reproduced unchanged.
pub const OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"eth-up-or-down-daily-1788105600","orderbook":{"bids":[{"price":0.012,"size":83334000,"side":"BUY"},{"price":0.011,"size":100000000,"side":"BUY"},{"price":0.01,"size":21000000,"side":"BUY"},{"price":0.009,"size":1000000,"side":"BUY"},{"price":0.008,"size":1000000,"side":"BUY"},{"price":0.007,"size":1000000,"side":"BUY"},{"price":0.006,"size":167000000,"side":"BUY"},{"price":0.005,"size":1201000000,"side":"BUY"},{"price":0.002,"size":50000000,"side":"BUY"},{"price":0.001,"size":2000000000,"side":"BUY"}],"asks":[{"price":0.219,"size":100000000,"side":"SELL"},{"price":0.22,"size":12000000,"side":"SELL"},{"price":0.239,"size":100000000,"side":"SELL"},{"price":0.249,"size":100000000,"side":"SELL"},{"price":0.259,"size":100000000,"side":"SELL"},{"price":0.27,"size":5000000,"side":"SELL"},{"price":0.279,"size":100000000,"side":"SELL"},{"price":0.293,"size":100000000,"side":"SELL"},{"price":0.306,"size":1441000,"side":"SELL"},{"price":0.65,"size":185714000,"side":"SELL"},{"price":0.969,"size":50000000,"side":"SELL"},{"price":0.989,"size":100000000,"side":"SELL"},{"price":0.99,"size":21000000,"side":"SELL"},{"price":0.991,"size":1000000,"side":"SELL"},{"price":0.992,"size":1000000,"side":"SELL"},{"price":0.993,"size":1000000,"side":"SELL"},{"price":0.994,"size":1000000,"side":"SELL"},{"price":0.995,"size":1000000,"side":"SELL"},{"price":0.998,"size":550000000,"side":"SELL"},{"price":0.999,"size":2000000000,"side":"SELL"}],"tokenId":"25018063611559838047404811982184442876005199660833597814711111046007291893507","adjustedMidpoint":0.115,"midpoint":0.1155,"maxSpread":0.035,"minSize":100000000},"version":7861372,"timestamp":"2026-08-31T07:12:32.741Z"}]"#;

/// An `oraclePriceData` event the venue emits on the market room; not a family the venue documents, so the native rail admits it as a bounded unknown envelope.
pub const OBSERVED_ORACLE_PRICE_DATA: &str = r#"42/markets,["oraclePriceData",{"marketAddress":null,"marketSlug":"eth-up-or-down-daily-1788105600","source":"pyth-pro","timestamp":1788160353000,"value":2440.91810993}]"#;
