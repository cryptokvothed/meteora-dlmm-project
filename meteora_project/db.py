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

    # Open the SQL file
    with open("db.sql", 'r') as sql_file:
        sql_script = sql_file.read()
        sql_commands = sql_script.split(';')

        # Execute SQL commands
        for sql in sql_commands:
            try:
                cursor.execute(sql)
                conn.commit()
            except sqlite3.OperationalError as e:
                logger.warning("SQL command failed: %s", sql)
                logger.warning("Error: %s", e)

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
    logger.debug("DLMM pair added to database: %s", dlmm_pair_tuple[2])


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
