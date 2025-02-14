CREATE TABLE IF NOT EXISTS tokens (
  mint VARCHAR(44) NOT NULL PRIMARY KEY,
  symbol VARCHAR
);
CREATE TABLE IF NOT EXISTS pairs (
  pair_address VARCHAR(44) NOT NULL PRIMARY KEY,
  name VARCHAR NOT NULL,
  mint_x VARCHAR(44) NOT NULL REFERENCES tokens(mint),
  mint_y VARCHAR(44) NOT NULL REFERENCES tokens(mint),
  bin_step INTEGER NOT NULL,
  base_fee_percentage DOUBLE NOT NULL,
  hide BOOLEAN DEFAULT FALSE NOT NULL,
  is_blacklisted BOOLEAN DEFAULT FALSE NOT NULL,
  cumulative_fee_volume DOUBLE NOT NULL
);
CREATE TABLE IF NOT EXISTS pair_history (
  created_at TIMESTAMP NOT NULL,
  pair_address VARCHAR(44) NOT NULL REFERENCES pairs(pair_address),
  price DOUBLE NOT NULL,
  liquidity DOUBLE NOT NULL,
  fees DOUBLE
);
CREATE INDEX IF NOT EXISTS pair_history_update_id_IDX ON pair_history (created_at);
CREATE INDEX IF NOT EXISTS pair_history_dlmm_pair_id_IDX ON pair_history (pair_address);
CREATE INDEX IF NOT EXISTS pair_history_update_id_dlmm_pair_id_IDX ON pair_history(created_at, pair_address);
CREATE VIEW IF NOT EXISTS v_pair_stats AS WITH pair_stats AS (
  SELECT p.name,
    p.pair_address,
    p.bin_step,
    p.base_fee_percentage,
    count(*) num_minutes,
    count(*) FILTER (fees > 0) num_minutes_with_volume,
    round(100 * num_minutes_with_volume / num_minutes) pct_minutes_with_volume,
    last(
      h.price
      ORDER BY created_at
    ) current_price,
    avg(h.price) average_price,
    min(h.price) min_price,
    max(h.price) max_price,
    100 * (max_price - current_price) / current_price pct_below_max,
    pct_below_max < 5 near_max,
    100 * (max_price - min_price) / min_price price_range_pct,
    COALESCE(
      NULLIF(
        ceil(
          100 * price_range_pct / last(
            p.bin_step
            ORDER BY created_at
          )
        ),
        0
      ),
      1
    ) bins_range,
    ceil(bins_range / 69) num_positions_range,
    stddev_samp(h.price) price_std_dev,
    price_std_dev / average_price price_volatility_ratio,
    last(
      h.liquidity
      ORDER BY created_at
    ) current_liquidity,
    min(h.liquidity) min_liquidity,
    max(h.liquidity) max_liquidity,
    avg(h.liquidity) avg_liquidity,
    stddev_samp(h.liquidity) liquidity_std_dev,
    liquidity_std_dev / avg_liquidity liquidity_volatility_ratio,
    sum(h.fees) total_fees,
    100 * total_fees / (avg_liquidity + liquidity_std_dev) fees_tvl_pct,
    60 * 24 / num_minutes * fees_tvl_pct fees_tvl_24h_pct
  FROM pair_history h
    JOIN pairs p ON h.pair_address = p.pair_address
  WHERE NOT p.is_blacklisted
    AND created_at >= (
      SELECT MAX(created_at)
      FROM pair_history
    ) - INTERVAL 30 MINUTE
  GROUP BY ALL
)
FROM pair_stats
WHERE NOT isnan(fees_tvl_pct)
  AND NOT isinf(fees_tvl_pct)
  AND pct_minutes_with_volume > 75
  AND total_fees > 100
  AND current_liquidity > 1000
ORDER BY fees_tvl_pct DESC;