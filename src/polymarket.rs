//! Polymarket venue adapter: native-event decoding and the upstream connection rail.
//!
//! The adapter decodes only the documented public WebSocket vocabulary. It preserves source
//! snapshots and source-reported level changes as distinct events, and treats timestamps
//! and hashes as reported diagnostics rather than ordering or continuity evidence.

pub mod native;
pub(crate) mod upstream;

/// Polymarket's documented unauthenticated public market-channel endpoint.
pub const DEFAULT_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// The largest payload one decoded market-channel message may carry.
pub(crate) const MAX_PAYLOAD_BYTES: usize = crate::wire::session::MAX_PAYLOAD_BYTES_MAX as usize;
