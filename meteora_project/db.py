# db.py

import sqlite3
import json
from datetime import datetime, timezone
import logging
import config

logger = logging.getLogger(__name__)

def setup_database(db_name=config.DB_FILENAME):
    """
    Connects to the SQLite database and creates the 'api_entries' table if it doesn't exist.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            api_returned_at TEXT,
            address TEXT,
            name TEXT,
            mint_x TEXT,
            mint_y TEXT,
            reserve_x TEXT,
            reserve_y TEXT,
            reserve_x_amount INTEGER,
            reserve_y_amount INTEGER,
            bin_step INTEGER,
            base_fee_percentage TEXT,
            max_fee_percentage TEXT,
            protocol_fee_percentage TEXT,
            liquidity TEXT,
            reward_mint_x TEXT,
            reward_mint_y TEXT,
            fees_24h REAL,
            today_fees REAL,
            trade_volume_24h REAL,
            cumulative_trade_volume TEXT,
            cumulative_fee_volume TEXT,
            current_price REAL,
            apr REAL,
            apy REAL,
            farm_apr REAL,
            farm_apy REAL,
            hide BOOLEAN,
            is_blacklisted BOOLEAN,
            fees TEXT,
            fee_tvl_ratio TEXT
        )
    ''')
    conn.commit()
    logger.info("Database setup complete.")
    return conn

def insert_api_entry(conn, entry, api_timestamp):
    """
    Inserts a single API entry (a dict) into the SQLite database.
    
    Two datetime fields are recorded:
      - created_at: when the insertion is done.
      - api_returned_at: when the API call returned data.
      
    Nested dictionaries (e.g., 'fees' and 'fee_tvl_ratio') are stored as JSON strings.
    """
    cursor = conn.cursor()
    created_at = datetime.now(timezone.utc).isoformat()  # Insertion timestamp

    # Convert nested dicts to JSON strings
    fees_json = json.dumps(entry.get("fees", {}))
    fee_tvl_ratio_json = json.dumps(entry.get("fee_tvl_ratio", {}))

    # Construct the data tuple with exactly 31 values.
    data_tuple = (
        created_at,                           # 1. created_at
        api_timestamp,                        # 2. api_returned_at
        entry.get("address"),                 # 3. address
        entry.get("name"),                    # 4. name
        entry.get("mint_x"),                  # 5. mint_x
        entry.get("mint_y"),                  # 6. mint_y
        entry.get("reserve_x"),               # 7. reserve_x
        entry.get("reserve_y"),               # 8. reserve_y
        entry.get("reserve_x_amount"),        # 9. reserve_x_amount
        entry.get("reserve_y_amount"),        # 10. reserve_y_amount
        entry.get("bin_step"),                # 11. bin_step
        entry.get("base_fee_percentage"),     # 12. base_fee_percentage
        entry.get("max_fee_percentage"),      # 13. max_fee_percentage
        entry.get("protocol_fee_percentage"), # 14. protocol_fee_percentage
        entry.get("liquidity"),               # 15. liquidity
        entry.get("reward_mint_x"),           # 16. reward_mint_x
        entry.get("reward_mint_y"),           # 17. reward_mint_y
        entry.get("fees_24h"),                # 18. fees_24h
        entry.get("today_fees"),              # 19. today_fees
        entry.get("trade_volume_24h"),        # 20. trade_volume_24h
        entry.get("cumulative_trade_volume"), # 21. cumulative_trade_volume
        entry.get("cumulative_fee_volume"),   # 22. cumulative_fee_volume
        entry.get("current_price"),           # 23. current_price
        entry.get("apr"),                     # 24. apr
        entry.get("apy"),                     # 25. apy
        entry.get("farm_apr"),                # 26. farm_apr
        entry.get("farm_apy"),                # 27. farm_apy
        entry.get("hide"),                    # 28. hide
        entry.get("is_blacklisted"),          # 29. is_blacklisted
        fees_json,                            # 30. fees (as JSON string)
        fee_tvl_ratio_json                    # 31. fee_tvl_ratio (as JSON string)
    )

    # Debug print to verify the number of values (should print 31)
    print("Number of values to insert:", len(data_tuple))
    
    cursor.execute('''
        INSERT INTO api_entries (
            created_at,
            api_returned_at,
            address,
            name,
            mint_x,
            mint_y,
            reserve_x,
            reserve_y,
            reserve_x_amount,
            reserve_y_amount,
            bin_step,
            base_fee_percentage,
            max_fee_percentage,
            protocol_fee_percentage,
            liquidity,
            reward_mint_x,
            reward_mint_y,
            fees_24h,
            today_fees,
            trade_volume_24h,
            cumulative_trade_volume,
            cumulative_fee_volume,
            current_price,
            apr,
            apy,
            farm_apr,
            farm_apy,
            hide,
            is_blacklisted,
            fees,
            fee_tvl_ratio
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data_tuple)
    conn.commit()
    logger.info("Entry inserted into database.")
