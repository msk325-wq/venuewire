//! Table DDL per spec §7.3, issued idempotently via `CREATE TABLE IF NOT
//! EXISTS` at sink startup.

/// The `ticks` table, exactly as specified in §7.3.
pub const TICKS_DDL: &str = "\
CREATE TABLE IF NOT EXISTS ticks (
  instrument String, venue LowCardinality(String),
  yes_bid Nullable(Decimal(9,6)), yes_ask Nullable(Decimal(9,6)),
  last_price Nullable(Decimal(9,6)),
  venue_ts Nullable(DateTime64(3, 'UTC')),
  recv_ts DateTime64(3, 'UTC'), seq UInt64
) ENGINE = MergeTree ORDER BY (instrument, recv_ts)";

/// The `divergences` table mirroring `DivergenceEvent`: legs are stored as a
/// JSON string (`[[instrument, mid], ...]`).
pub const DIVERGENCES_DDL: &str = "\
CREATE TABLE IF NOT EXISTS divergences (
  match_id String, spread Decimal(9,6),
  legs String,
  detected_at DateTime64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (match_id, detected_at)";
