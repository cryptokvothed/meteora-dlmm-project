CREATE TABLE IF NOT EXISTS tokens (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  address TEXT(44) NOT NULL,
  symbol TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS tokens_address_IDX ON tokens (address);
CREATE TABLE IF NOT EXISTS dlmm_pairs (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  address TEXT(44) NOT NULL,
  name TEXT NOT NULL,
  mint_x_id INTEGER NOT NULL,
  mint_y_id INTEGER NOT NULL,
  bin_step INTEGER NOT NULL,
  base_fee_percentage REAL NOT NULL,
  hide INTEGER DEFAULT (0) NOT NULL,
  is_blacklisted INTEGER DEFAULT (0) NOT NULL,
  tracked_volume REAL DEFAULT (0) NOT NULL,
  tracked_fees REAL DEFAULT (0) NOT NULL,
  CONSTRAINT dlmm_pairs_x_tokens_FK FOREIGN KEY (mint_x_id) REFERENCES tokens(id) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT dlmm_pairs_y_tokens_FK FOREIGN KEY (mint_y_id) REFERENCES tokens(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS dlmm_pairs_address_IDX ON dlmm_pairs (address);
CREATE INDEX IF NOT EXISTS dlmm_pairs_tracked_fees_IDX ON dlmm_pairs (tracked_fees);
CREATE INDEX IF NOT EXISTS dlmm_pairs_fees_id_IDX ON dlmm_pairs (tracked_fees, id);
CREATE TABLE IF NOT EXISTS dlmm_pair_history_updates (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS dlmm_pair_history (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  update_id INTEGER NOT NULL,
  dlmm_pair_id INTEGER NOT NULL,
  price REAL NOT NULL,
  liquidity REAL NOT NULL,
  cumulative_trade_volume REAL NOT NULL,
  cumulative_fee_volume REAL NOT NULL,
  minutes_since_last_update REAL,
  volume REAL,
  fee_volume REAL,
  CONSTRAINT dlmm_pair_history_dlmm_pairs_FK FOREIGN KEY (dlmm_pair_id) REFERENCES dlmm_pairs(id) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT dlmm_pair_history_dlmm_pair_history_updates_FK FOREIGN KEY (update_id) REFERENCES dlmm_pair_history_updates(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
CREATE INDEX IF NOT EXISTS dlmm_pair_history_update_id_IDX ON dlmm_pair_history (update_id);
CREATE INDEX IF NOT EXISTS dlmm_pair_history_dlmm_pair_id_IDX ON dlmm_pair_history (dlmm_pair_id);
CREATE INDEX IF NOT EXISTS dlmm_pair_history_update_id_dlmm_pair_id_IDX ON dlmm_pair_history(update_id, dlmm_pair_id);
CREATE VIEW v_dlmm_opportunities AS WITH history AS (
  SELECT h.update_id,
    DATETIME(u.created_at, 'unixepoch') update_time,
    h.dlmm_pair_id pair_id,
    p.address pair_address,
    p.name pair_name,
    p.bin_step,
    p.base_fee_percentage,
    h.minutes_since_last_update,
    h.price,
    h.liquidity,
    h.volume,
    h.fee_volume
  FROM dlmm_pair_history h
    JOIN dlmm_pairs p ON h.dlmm_pair_id = p.id
    JOIN dlmm_pair_history_updates u ON h.update_id = u.id
  WHERE h.update_id > 1
),
pair_stats AS (
  SELECT pair_address,
    pair_name,
    bin_step,
    base_fee_percentage,
    SUM(ROUND(minutes_since_last_update)) total_minutes,
    SUM(
      CASE
        WHEN fee_volume > 0 THEN 1
        ELSE 0
      END
    ) num_minutes_with_volume,
    ROUND(
      100.0 * SUM(
        CASE
          WHEN fee_volume > 0 THEN 1
          ELSE 0
        END
      ) / COUNT(*)
    ) pct_minutes_with_volume,
    MIN(price) min_price,
    MAX(price) max_price,
    AVG(price) avg_price,
    ROUND(10000 * (MAX(price) - MIN(PRICE)) / MIN(PRICE)) bps_range,
    CEIL(
      10000 * (MAX(price) - MIN(PRICE)) / MIN(PRICE) / bin_step
    ) bins_range,
    CEIL(
      10000 * (MAX(price) - MIN(PRICE)) / MIN(PRICE) / bin_step / 69
    ) num_positions,
    AVG(liquidity) avg_liquidity,
    ROUND(SUM(fee_volume), 2) fees,
    ROUND(
      100 * SUM(fee_volume * minutes_since_last_update) / SUM(liquidity * minutes_since_last_update),
      2
    ) time_weighted_fees_tvl,
    ROUND(
      100 * 60 * 24 / SUM(minutes_since_last_update) * SUM(fee_volume * minutes_since_last_update) / SUM(liquidity * minutes_since_last_update),
      2
    ) time_weighted_fees_tvl_projected_24h
  FROM history
  WHERE update_id > (
      SELECT MAX(id)
      from dlmm_pair_history_updates
    ) - 30
  GROUP BY pair_address,
    pair_name,
    bin_step,
    base_fee_percentage
)
SELECT *
FROM pair_stats
WHERE pct_minutes_with_volume > 50
  AND fees > 100
  AND total_minutes >= 29
ORDER BY time_weighted_fees_tvl_projected_24h DESC;