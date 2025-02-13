# db.py

import os
import sqlite3
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
    working_dir = os.path.dirname(os.path.abspath(__file__))
    with open(working_dir + "/db.sql", 'r') as sql_file:
        sql_script = sql_file.read()
        sql_commands = sql_script.split(';')

        # Execute SQL commands
        for sql in sql_commands:
            try:
                if (sql.strip() != ""):
                    cursor.execute(sql)
                    conn.commit()
            except sqlite3.OperationalError as e:
                logger.warning("SQL command failed: %s", sql)
                logger.warning("Error: %s", e)

    # Open the triggers SQL file
    with open(working_dir + "/db-triggers.sql", "r") as trigger_file:
        trigger_script = trigger_file.read()
        trigger_commands = trigger_script.split('END;')

        # Execute SQL commands
        for trigger_sql in trigger_commands:
            try:
                if (trigger_sql.strip() != ""):
                    cursor.execute(trigger_sql + "\nEND;")
                    conn.commit()
            except sqlite3.OperationalError as e:
                logger.warning("SQL command failed: %s", trigger_sql)
                logger.warning("Error: %s", e)

    logger.info("Database setup complete.")
    return conn

def insert_meteora_api_entries(conn, entries, created_at):
    """
    Reads data from the Meteora API and inserts into the SQLite tables in batches.
    """
    cursor = conn.cursor()

    # Add the update to the dlmm_pair_history_updates table
    cursor.execute('''
        INSERT INTO dlmm_pair_history_updates (created_at)
        VALUES (?)
    ''', (created_at,))
    conn.commit()

    # Query the dlmm_pair_history to get the history update id
    cursor.execute('SELECT MAX(id) FROM dlmm_pair_history_updates')
    update_id = cursor.fetchone()[0]

    # Prepare lists for batch inserts
    tokens = []
    dlmm_pairs = []
    dlmm_pair_meteora_histories = []

    for entry in entries:
        # Get the symbols from the market name
        symbols = entry.get("name").split("-")

        # Add the x token
        tokens.append((created_at, entry.get("mint_x"), symbols[0]))

        # Add the y token
        tokens.append((created_at, entry.get("mint_y"), symbols[1]))

        # Create the dlmm_pair tuple
        dlmm_pair_tuple = (
            created_at,
            entry.get("address"),
            entry.get("name"),
            entry.get("mint_x"),  # Will be replaced with the database id
            entry.get("mint_y"),  # Will be replaced with the database id
            entry.get("bin_step"),
            float(entry.get("base_fee_percentage")),
            int(entry.get("hide", 0)),
            int(entry.get("is_blacklisted", 0))
        )
        dlmm_pairs.append(dlmm_pair_tuple)

        # Create the dlmm_pair_history tuple
        dlmm_pair_history_tuple = (
            update_id,
            entry.get("address"),  # Will be replaced with the database id
            entry.get("current_price"),
            float(entry.get("liquidity")),
            float(entry.get("cumulative_trade_volume")),
            float(entry.get("cumulative_fee_volume"))
        )
        dlmm_pair_meteora_histories.append(dlmm_pair_history_tuple)

    # Insert tokens in batch
    cursor.executemany('''
        INSERT OR IGNORE INTO tokens (
            created_at,
            address,
            symbol
        )
        VALUES (?, ?, ?)
    ''', tokens)
    conn.commit()

    # Retrieve token ids and update dlmm_pairs
    # Retrieve all token ids in a single query
    cursor.execute('SELECT address, id FROM tokens')
    token_id_map = {address: token_id for address, token_id in cursor.fetchall()}

    # Update dlmm_pairs with token ids
    for i, dlmm_pair in enumerate(dlmm_pairs):
        mint_x_address = dlmm_pair[3]
        mint_y_address = dlmm_pair[4]
        mint_x_id = token_id_map[mint_x_address]
        mint_y_id = token_id_map[mint_y_address]
        dlmm_pairs[i] = (
            dlmm_pair[0],
            dlmm_pair[1],
            dlmm_pair[2],
            mint_x_id,
            mint_y_id,
            dlmm_pair[5],
            dlmm_pair[6],
            dlmm_pair[7],
            dlmm_pair[8]
        )

    # Insert dlmm_pairs in batch
    cursor.executemany('''
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
    ''', dlmm_pairs)
    conn.commit()

    # Retrieve all dlmm_pair ids in a single query
    cursor.execute('SELECT address, id FROM dlmm_pairs')
    dlmm_pair_id_map = {address: dlmm_pair_id for address, dlmm_pair_id in cursor.fetchall()}

    # Update dlmm_pair_meteora_histories with dlmm_pair ids
    for i, dlmm_pair_history in enumerate(dlmm_pair_meteora_histories):
        dlmm_pair_address = dlmm_pair_history[1]
        dlmm_pair_id = dlmm_pair_id_map[dlmm_pair_address]
        dlmm_pair_meteora_histories[i] = (
            dlmm_pair_history[0],
            dlmm_pair_id,
            dlmm_pair_history[2],
            dlmm_pair_history[3],
            dlmm_pair_history[4],
            dlmm_pair_history[5]
        )

    # Insert dlmm_pair_meteora_histories in batch
    cursor.executemany('''
        INSERT INTO dlmm_pair_history (
            update_id,
            dlmm_pair_id,
            price,
            liquidity,
            cumulative_trade_volume,
            cumulative_fee_volume
        )
        VALUES (?, ?, ?, ?, ?, ?)
    ''', dlmm_pair_meteora_histories)
    conn.commit()