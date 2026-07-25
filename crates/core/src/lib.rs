//! Canonical types, configuration, and shared utilities for venuewire.
//!
//! Everything that crosses a crate boundary lives here: the normalized tick
//! schema, instrument metadata, cross-venue match types, and the config model.

pub mod config;
pub mod telemetry;
pub mod types;

pub use config::Config;
pub use types::{
    DivergenceEvent, Instrument, InstrumentId, MatchConfidence, MatchMethod, MatchedMarket,
    RawFrame, Tick, Venue,
};
