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

    # Create the tokens table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            address TEXT(44) NOT NULL,
            symbol TEXT
        )
    ''')
    conn.commit()
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS tokens_address_IDX ON tokens (address)
    ''')
    conn.commit()

    # Create the dlmm_pairs table
    cursor.execute('''
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
        )
    ''')
    conn.commit()
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS dlmm_pairs_address_IDX ON dlmm_pairs (address)
    ''')
    conn.commit()

    # Create the dlmm_pair_meteora_history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dlmm_pair_meteora_history (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            dlmm_pair_id INTEGER NOT NULL,
            price REAL NOT NULL,
            liquidity REAL NOT NULL,
            cumulative_trade_volume REAL NOT NULL,
            cumulative_fee_volume REAL NOT NULL,
            CONSTRAINT dlmm_pair_meteora_history_dlmm_pairs_FK FOREIGN KEY (dlmm_pair_id) REFERENCES dlmm_pairs(id) ON DELETE CASCADE ON UPDATE RESTRICT
        )
    ''')
    conn.commit()

    # Create a view to join all the tables
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS v_dlmm_history AS
        WITH history AS (
            SELECT 
                h.created_at,
                DATETIME(h.created_at, 'unixepoch') iso_date,
                p.name AS pair_name,
                p.address AS pair_address,
                p.bin_step,
                p.base_fee_percentage,
                h.price,
                h.liquidity,
                h.cumulative_trade_volume,
                h.cumulative_fee_volume
            FROM 
                dlmm_pair_meteora_history h
                JOIN dlmm_pairs p ON h.dlmm_pair_id = p.id
                JOIN tokens t_x ON p.mint_x_id = t_x.id
                JOIN tokens t_y ON p.mint_y_id = t_y.id
            WHERE 
                is_blacklisted = 0
        ),
        prior_history_records AS (
            SELECT
                created_at,
                iso_date,
                pair_name,
                pair_address,
                bin_step,
                bin_step base_fee_percentage,
                LAG(created_at) OVER (PARTITION BY pair_address ORDER BY created_at) prior_created_at,
                LAG(price) OVER (PARTITION BY pair_address ORDER BY created_at) prior_price,
                LAG(liquidity) OVER (PARTITION BY pair_address ORDER BY created_at) prior_liquidity,
                LAG(cumulative_trade_volume) OVER (PARTITION BY pair_address ORDER BY created_at) prior_cumulative_trade_volume,
                LAG(cumulative_fee_volume) OVER (PARTITION BY pair_address ORDER BY created_at) prior_cumulative_fee_volume,
                price,
                liquidity,
                cumulative_trade_volume,
                cumulative_fee_volume
            FROM
                history
            ORDER BY
                pair_address,
                created_at
        )
        SELECT 
            created_at,
            iso_date,
            pair_name,
            pair_address,
            bin_step,
            bin_step base_fee_percentage,
            (created_at - prior_created_at) minutes_elapsed,
            (liquidity + prior_liquidity) / 2 liquidity,
            cumulative_trade_volume - prior_cumulative_trade_volume volume,
            cumulative_fee_volume - prior_cumulative_fee_volume fees,
            (cumulative_fee_volume - prior_cumulative_fee_volume) / ((liquidity + prior_liquidity) / 2) * 60 * 60 * 24 / (created_at - prior_created_at) fee_liquity_24h
        FROM 
            prior_history_records
        WHERE
            prior_created_at IS NOT NULL
    ''')
    conn.commit()

    logger.info("Database setup complete.")
    return conn

def insert_meteora_api_entry(conn, entry, api_timestamp):
    """
    Reads data from the Meteora API and inserts into the SQLite tables
    
    """
    created_at = datetime.now(timezone.utc).timestamp()  # Convert to unix

    # Get the symbols from the market name
    symbols = entry.get("name").split("-")

    # Add the x token
    mint_x_id = add_token(conn, created_at, entry.get("mint_x"), symbols[0])

    # Add the y token
    mint_y_id = add_token(conn, created_at, entry.get("mint_y"), symbols[1])

    # Create the dlmm_pair tuple
    dlmm_pair_tuple = (
        created_at,
        entry.get("address"),
        entry.get("name"),
        mint_x_id,
        mint_y_id,
        entry.get("bin_step"),
        float(entry.get("base_fee_percentage")),
        int(entry.get("hide", 0)),
        int(entry.get("is_blacklisted", 0))
    )

    # Insert the dlmm_pair into the database
    dlmm_pair_id = add_dlmm_pair(conn, dlmm_pair_tuple)

    # Create the dlmm_pair_meteora_history tuple
    dlmm_pair_meteora_history_tuple = (
        created_at,
        dlmm_pair_id,
        entry.get("current_price"),
        float(entry.get("liquidity")),
        float(entry.get("cumulative_trade_volume")),
        float(entry.get("cumulative_fee_volume"))
    )

    # Insert the history data into the dlmm_pair_meteora_history table
    add_dlmm_pair_meteora_history(conn, dlmm_pair_meteora_history_tuple)
    logger.info("DLMM pair added: %s", dlmm_pair_tuple[2])


def add_token(conn, created_at, address, symbol):
    """
    Adds a token to the database.
    """
    cursor = conn.cursor()

    # Insert the token into the tokens table
    cursor.execute('''
        INSERT OR IGNORE INTO tokens (
            created_at,
            address,
            symbol
        )
        VALUES (?, ?, ?)
    ''', (created_at, address, symbol))
    conn.commit()

    # Get the token id and return it
    cursor.execute('''
        SELECT id FROM tokens WHERE address = ?
    ''', (address,))
    result = cursor.fetchone()
    return result[0] if result else None    

def add_dlmm_pair(conn, dlmm_pair_tuple):
    """
    Adds a DLMM pair to the database.
    """
    cursor = conn.cursor()

    # Insert the DLMM pair into the dlmm_pairs table
    cursor.execute('''
        INSERT OR IGNORE INTO dlmm_pairs (
            created_at,
            address,
            name,
            mint_x_id,
            mint_y_id,
            bin_step,
            base_fee_percentage,
            hide,
            is_blacklisted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', dlmm_pair_tuple)
    conn.commit()

    # Get the DLMM pair id and return it
    cursor.execute('''
        SELECT id FROM dlmm_pairs WHERE address = ?
    ''', (dlmm_pair_tuple[1],))
    result = cursor.fetchone()
    return result[0] if result else None

def add_dlmm_pair_meteora_history(conn, dlmm_pair_meteora_history_tuple):
    """
    Adds a DLMM pair meteora history entry to the database.
    """
    cursor = conn.cursor()

    # Insert the DLMM pair meteora history entry into the dlmm_pair_meteora_history table
    cursor.execute('''
        INSERT INTO dlmm_pair_meteora_history (
            created_at,
            dlmm_pair_id,
            price,
            liquidity,
            cumulative_trade_volume,
            cumulative_fee_volume
        )
        VALUES (?, ?, ?, ?, ?, ?)
    ''', dlmm_pair_meteora_history_tuple)
    conn.commit()
