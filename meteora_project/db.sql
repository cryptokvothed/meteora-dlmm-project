CREATE TABLE IF NOT EXISTS tokens (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  address TEXT(44) NOT NULL,
  symbol TEXT
);
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
  CONSTRAINT dlmm_pairs_x_tokens_FK FOREIGN KEY (mint_x_id) REFERENCES tokens(id) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT dlmm_pairs_y_tokens_FK FOREIGN KEY (mint_y_id) REFERENCES tokens(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS dlmm_pairs_address_IDX ON dlmm_pairs (address);
CREATE TABLE IF NOT EXISTS dlmm_pair_meteora_history (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  dlmm_pair_id INTEGER NOT NULL,
  price REAL NOT NULL,
  liquidity REAL NOT NULL,
  cumulative_trade_volume REAL NOT NULL,
  cumulative_fee_volume REAL NOT NULL,
  CONSTRAINT dlmm_pair_meteora_history_dlmm_pairs_FK FOREIGN KEY (dlmm_pair_id) REFERENCES dlmm_pairs(id) ON DELETE CASCADE ON UPDATE RESTRICT
);
DROP VIEW IF EXISTS v_dlmm_pairs;
CREATE VIEW IF NOT EXISTS v_dlmm_history AS WITH history AS (
  SELECT h.created_at,
    DATETIME(h.created_at, 'unixepoch') iso_date,
    p.name AS pair_name,
    p.address AS pair_address,
    p.bin_step,
    p.base_fee_percentage,
    h.price,
    h.liquidity,
    h.cumulative_trade_volume,
    h.cumulative_fee_volume
  FROM dlmm_pair_meteora_history h
    JOIN dlmm_pairs p ON h.dlmm_pair_id = p.id
    JOIN tokens t_x ON p.mint_x_id = t_x.id
    JOIN tokens t_y ON p.mint_y_id = t_y.id
  WHERE is_blacklisted = 0
),
prior_history_records AS (
  SELECT created_at,
    iso_date,
    pair_name,
    pair_address,
    bin_step,
    bin_step base_fee_percentage,
    LAG(created_at) OVER (
      PARTITION BY pair_address
      ORDER BY created_at
    ) prior_created_at,
    LAG(price) OVER (
      PARTITION BY pair_address
      ORDER BY created_at
    ) prior_price,
    LAG(liquidity) OVER (
      PARTITION BY pair_address
      ORDER BY created_at
    ) prior_liquidity,
    LAG(cumulative_trade_volume) OVER (
      PARTITION BY pair_address
      ORDER BY created_at
    ) prior_cumulative_trade_volume,
    LAG(cumulative_fee_volume) OVER (
      PARTITION BY pair_address
      ORDER BY created_at
    ) prior_cumulative_fee_volume,
    price,
    liquidity,
    cumulative_trade_volume,
    cumulative_fee_volume
  FROM history
  ORDER BY pair_address,
    created_at
),
elapsed_time AS (
  SELECT created_at,
    iso_date,
    pair_name,
    pair_address,
    bin_step,
    bin_step base_fee_percentage,
    (created_at - prior_created_at) / 60 minutes_elapsed,
    (liquidity + prior_liquidity) / 2 liquidity,
    cumulative_trade_volume - prior_cumulative_trade_volume volume,
    cumulative_fee_volume - prior_cumulative_fee_volume fees
  FROM prior_history_records
  WHERE prior_created_at IS NOT NULL
)
SELECT created_at,
  iso_date,
  pair_name,
  pair_address,
  bin_step,
  base_fee_percentage,
  minutes_elapsed,
  SUM(minutes_elapsed) OVER (
    PARTITION BY pair_address
    ORDER BY created_at
  ) total_minutes_elapsed,
  liquidity,
  volume,
  fees
FROM elapsed_time;
DROP VIEW IF EXISTS v_dlmm_opportunities;
CREATE VIEW IF NOT EXISTS v_dlmm_opportunities AS WITH aggregates_by_pair AS (
  SELECT pair_name,
    pair_address,
    MIN(created_at) first_update,
    MAX(created_at) last_update,
    ROUND(SUM(minutes_elapsed), 0) minutes_elapsed,
    ROUND(SUM(volume), 2) volume,
    ROUND(SUM(fees), 2) fees,
    ROUND(
      SUM(
        CASE
          WHEN volume > 0 THEN 1.0
          ELSE 0.0
        END
      ) / COUNT(),
      3
    ) * 100.0 pct_minutes_with_volume,
    ROUND(MIN(liquidity), 2) min_liquidity,
    ROUND(MAX(liquidity), 2) max_liquidity,
    ROUND(
      SUM(liquidity * minutes_elapsed) / SUM(minutes_elapsed),
      2
    ) avg_liquidity,
    ROUND(SUM(fees) / AVG(liquidity) * 100, 2) total_fee_avg_liquidity,
    ROUND(
      SUM(fees * minutes_elapsed) / SUM(liquidity * minutes_elapsed) * 1440 * 100,
      2
    ) fee_liquidity_pct_24h
  FROM v_dlmm_history
  WHERE total_minutes_elapsed >= 15
  GROUP BY pair_name,
    pair_address
  ORDER BY SUM(fees * minutes_elapsed) / SUM(liquidity * minutes_elapsed) DESC
)
SELECT *
FROM aggregates_by_pair
WHERE pct_minutes_with_volume > 50
  AND minutes_elapsed = (
    SELECT MAX(minutes_elapsed)
    from aggregates_by_pair
  );