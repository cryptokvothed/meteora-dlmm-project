CREATE TRIGGER IF NOT EXISTS update_dlmm_pair_history_volume
AFTER
INSERT ON dlmm_pair_history FOR EACH ROW BEGIN
UPDATE dlmm_pair_history
SET volume = COALESCE(
    NEW.cumulative_trade_volume - (
      SELECT prior.cumulative_trade_volume
      FROM dlmm_pair_history AS prior
      WHERE prior.dlmm_pair_id = NEW.dlmm_pair_id
        AND prior.update_id = NEW.update_id - 1
    ),
    NULL
  ),
  fee_volume = COALESCE(
    NEW.cumulative_fee_volume - (
      SELECT prior.cumulative_fee_volume
      FROM dlmm_pair_history AS prior
      WHERE prior.dlmm_pair_id = NEW.dlmm_pair_id
        AND prior.update_id = NEW.update_id - 1
    ),
    NULL
  ),
  minutes_since_last_update = COALESCE(
    (
      (
        SELECT u.created_at
        FROM dlmm_pair_history_updates AS u
        WHERE u.id = NEW.update_id
      ) - (
        SELECT u.created_at
        FROM dlmm_pair_history_updates AS u
        WHERE u.id = NEW.update_id - 1
      )
    ) / 60,
    NULL
  )
WHERE id = NEW.id;
UPDATE dlmm_pairs
SET tracked_volume = tracked_volume + COALESCE(
    NEW.cumulative_trade_volume - (
      SELECT prior.cumulative_trade_volume
      FROM dlmm_pair_history AS prior
      WHERE prior.dlmm_pair_id = NEW.dlmm_pair_id
        AND prior.update_id = NEW.update_id - 1
    ),
    0
  ),
  tracked_fees = tracked_fees + COALESCE(
    NEW.cumulative_fee_volume - (
      SELECT prior.cumulative_fee_volume
      FROM dlmm_pair_history AS prior
      WHERE prior.dlmm_pair_id = NEW.dlmm_pair_id
        AND prior.update_id = NEW.update_id - 1
    ),
    0
  )
WHERE id = NEW.dlmm_pair_id;
END;