#![deny(unsafe_code)]
//! Venue-agnostic prediction-market data-feed library: the exact-decimal market-data
//! vocabulary and Socket.IO wire decoding for the pm-ws daemon.
//!
//! Invariants: authoritative prices and quantities are exact scaled integers, never
//! floats; public identity is the venue plus its native identifier; one writer per
//! authoritative book; queues are bounded; book authority is evidence-based, never
//! inactivity-timer based; venue-reported data is reproduced, never invented.

pub mod book;
pub mod control;
pub mod daemon;
pub mod dedup;
pub mod descriptor;
pub mod ffi;
pub mod identity;
pub mod limitless;
pub mod numeric;
pub mod observation;
pub mod observer;
pub mod pool;
pub mod shm;
pub mod state;
pub mod wire;

pub use book::*;
pub use control::*;
pub use daemon::*;
pub use dedup::*;
pub use descriptor::*;
pub use identity::*;
pub use numeric::*;
pub use observation::*;
pub use observer::*;
pub use pool::*;
pub use shm::*;
pub use state::*;
