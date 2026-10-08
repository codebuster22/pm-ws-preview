#![deny(unsafe_code)]
//! Venue-agnostic prediction-market native-event library: the exact-decimal numeric
//! vocabulary, Socket.IO and JSON wire decoding, and the complete venue-native event
//! documents the pm-ws upstream rail publishes.
//!
//! Invariants: venue decimals are parsed exactly into scaled integers, never floats;
//! unrepresentable precision is rejected rather than rounded; public identity is the venue
//! plus its native identifier; queues and admission gates are bounded and state their
//! overflow behavior; venue-reported data is reproduced, never invented; ingestion is
//! WebSocket-only and recovery is resubscribe then reconnect.

pub mod etiquette;
pub mod limitless;
pub mod native;
pub mod numeric;
pub mod peer;
pub mod polymarket;
pub mod upstream;
pub mod wire;

pub use numeric::*;
