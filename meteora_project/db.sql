-- Create sequences for auto-increment IDs
CREATE SEQUENCE IF NOT EXISTS tokens_id_seq;
CREATE SEQUENCE IF NOT EXISTS pairs_id_seq;
CREATE TABLE IF NOT EXISTS tokens (
  id INTEGER DEFAULT nextval('tokens_id_seq') PRIMARY KEY,
  mint VARCHAR(44) NOT NULL UNIQUE,
  symbol VARCHAR
);
CREATE INDEX IF NOT EXISTS tokens_mint_IDX ON tokens (mint);
CREATE TABLE IF NOT EXISTS pairs (
  id INTEGER DEFAULT nextval('pairs_id_seq') PRIMARY KEY,
  pair_address VARCHAR(44) NOT NULL UNIQUE,
  name VARCHAR NOT NULL,
  mint_x_id INTEGER NOT NULL REFERENCES tokens(id),
  mint_y_id INTEGER NOT NULL REFERENCES tokens(id),
  bin_step INTEGER NOT NULL,
  base_fee_percentage FLOAT NOT NULL,
  hide BOOLEAN DEFAULT FALSE NOT NULL,
  is_blacklisted BOOLEAN DEFAULT FALSE NOT NULL,
  cumulative_fee_volume FLOAT NOT NULL
);
CREATE INDEX IF NOT EXISTS pairs_pair_address_IDX ON pairs (pair_address);
CREATE TABLE IF NOT EXISTS pair_history (
  created_at TIMESTAMP NOT NULL,
  pair_id INTEGER NOT NULL REFERENCES pairs(id),
  price FLOAT NOT NULL,
  liquidity FLOAT NOT NULL,
  fees FLOAT
);
CREATE INDEX IF NOT EXISTS pair_history_update_id_IDX ON pair_history (created_at);
CREATE INDEX IF NOT EXISTS pair_history_update_id_dlmm_pair_id_IDX ON pair_history(created_at, pair_id);
CREATE VIEW IF NOT EXISTS v_pair_history AS WITH updates AS (
  SELECT DISTINCT created_at
  FROM pair_history
  ORDER BY created_at DESC
  LIMIT 60
), cumulative_stats AS (
  SELECT h.created_at,
    p.name,
    p.pair_address,
    p.bin_step,
    p.base_fee_percentage,
    h.price,
    h.liquidity,
    h.fees,
    count(*) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) num_minutes,
    avg(h.price) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) avg_price,
    sum(h.fees) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) cumulative_fees,
    avg(h.liquidity) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) avg_liquidity,
    round(
      stddev_samp(h.liquidity) OVER (
        PARTITION BY p.id
        ORDER BY created_at
      ),
      2
    ) liquidity_std_dev,
    round(liquidity_std_dev / avg_liquidity, 2) liquidity_volatility_ratio,
    CASE
      WHEN avg_liquidity = 0 THEN 0
      ELSE 100 * cumulative_fees / (avg_liquidity + liquidity_std_dev)
    END pct_geek_fees_liquidity,
    round(
      60 * 24 * pct_geek_fees_liquidity / num_minutes,
      2
    ) pct_geek_fees_liquidity_24h,
    count(*) FILTER (fees > 0) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) num_minutes_with_volume,
    round(100 * num_minutes_with_volume / num_minutes) pct_minutes_with_volume,
    coalesce(
      lag(h.price) OVER (
        PARTITION BY p.id
        ORDER BY created_at
      ) < h.price,
      false
    ) tick_up,
    coalesce(
      lag(h.price) OVER (
        PARTITION BY p.id
        ORDER BY created_at
      ) > h.price,
      false
    ) tick_down,
    min(h.price) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) min_price,
    max(h.price) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) max_price,
    round(100 * (max_price - min_price) / min_price, 2) pct_price_range,
    ceil(100 * pct_price_range / p.bin_step) bins_range,
    ceil(bins_range / 69) num_positions_range,
    100 * (max_price - h.price) / h.price pct_below_max,
    ceil(100 * pct_below_max / p.bin_step) bins_below_max,
    bins_below_max <= 7 near_max,
    stddev_samp(h.price) OVER (
      PARTITION BY p.id
      ORDER BY created_at
    ) std_dev_price,
    std_dev_price / avg_price price_volatility_ratio
  FROM pair_history h
    JOIN pairs p ON h.pair_id = p.id
  WHERE NOT p.is_blacklisted
    AND h.created_at IN (
      SELECT created_at
      from updates
    )
)
SELECT created_at dttm,
  name,
  pair_address,
  bin_step,
  base_fee_percentage,
  price,
  liquidity,
  fees,
  num_minutes,
  avg_price,
  cumulative_fees,
  avg_liquidity,
  liquidity_std_dev,
  liquidity_volatility_ratio,
  pct_geek_fees_liquidity,
  pct_geek_fees_liquidity_24h,
  stddev_samp(pct_geek_fees_liquidity_24h) OVER (
    PARTITION BY pair_address
    ORDER BY created_at
  ) std_dev_pct_geek_fees_liquidity_24h,
  std_dev_pct_geek_fees_liquidity_24h / pct_geek_fees_liquidity_24h pct_geek_fees_liquidity_24h_volatility_ratio,
  num_minutes_with_volume,
  pct_minutes_with_volume,
  sum(tick_up) OVER (
    PARTITION BY pair_address
    ORDER BY created_at
  ) num_tick_up,
  sum(tick_down) OVER (
    PARTITION BY pair_address
    ORDER BY created_at
  ) num_tick_down,
  num_tick_up / (num_tick_up + num_tick_down) pct_tick_up,
  min_price,
  max_price,
  pct_price_range,
  bins_range,
  num_positions_range,
  pct_below_max,
  bins_below_max,
  near_max,
  std_dev_price,
  price_volatility_ratio
FROM cumulative_stats;