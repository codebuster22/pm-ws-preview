//! Shared integration-test support: a controlled-peer fault-injection harness for the
//! Limitless Socket.IO `/markets` feed.
//!
//! Each integration-test binary compiles this module fresh, so `dead_code` is judged
//! per binary rather than across the whole surface this module offers to every
//! consumer. Allowed at this module level rather than per item, matching a test
//! binary that exercises only part of the API.
#![allow(dead_code)]

pub mod controlled_peer;
pub mod observed_frames;
