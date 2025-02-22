# db.py

import os
import duckdb
import pandas as pd
import logging
from meteora_project import config

logger = logging.getLogger(__name__)

def setup_database(db_name=config.DB_FILENAME):
    """
    Connects to the DuckDB database, creates tables, and returns the connection.
    """
    conn = duckdb.connect(db_name)

    # Open the SQL file
    working_dir = os.path.dirname(os.path.abspath(__file__))
    with open(working_dir + "/db.sql", 'r') as sql_file:
        sql_script = sql_file.read()
        sql_commands = sql_script.split(';')

        # Execute SQL commands
        for sql in sql_commands:
            try:
                if (sql.strip() != ""):
                    conn.sql(sql)
                    conn.commit()
            except duckdb.OperationalError as e:
                logger.warning("SQL command failed: %s", sql)
                logger.warning("Error: %s", e)

    logger.info("Database setup complete.")
    return conn

def insert_meteora_api_entries(conn, entries, created_at):
    """
    Reads data from the Meteora API and inserts into the DuckDB tables.
    """
    # Load the raw API entries into the database
    load_api_entries(conn, entries, created_at)

    # Add the x and y mints to the token table
    load_mints(conn)

    # Load the pairs
    load_pairs(conn)

    # Load the history
    load_history(conn)

    # Update the cumulative fees in the pairs table
    update_cumulative_fees(conn, created_at)

    # Unregister the api_entries table
    conn.unregister('api_entries')
    logger.debug("Data successfully fetched and inserted into the database.")

def load_api_entries(conn, entries, created_at):
    """
    Loads the raw API entries into the 'api_entries' table.
    """
    # Convert the entries to a DataFrame
    api_entries_df = pd.DataFrame(entries)

    # Add the 'created_at' column (which is a datetime type)
    api_entries_df['created_at'] = created_at

    # Split the name column into the x and y symbols
    split_columns = api_entries_df['name'].str.split('-', n=1, expand=True)
    api_entries_df['x'] = split_columns[0]
    api_entries_df['y'] = split_columns[1] if split_columns.shape[1] > 1 else None

    # Register the 'api_entries' table
    conn.register('api_entries', api_entries_df)

def load_mints(conn):
    """
    Loads the mint addresses into the 'tokens' table.
    """
    conn.execute('''
        INSERT INTO tokens (mint, symbol)
        SELECT 
            mint_x mint,
            x symbol 
        FROM api_entries 
        ON CONFLICT DO NOTHING
    ''')
    conn.execute('''
        INSERT INTO tokens (mint, symbol)
        SELECT 
            mint_y mint,
            x symbol 
        FROM api_entries 
        ON CONFLICT DO NOTHING
    ''')

def load_pairs(conn):
    """
    Loads the pairs into the 'pairs' table.
    """
    conn.execute('''
        INSERT INTO pairs (
            pair_address,
            name,
            mint_x_id,
            mint_y_id,
            bin_step,
            base_fee_percentage,
            hide,
            is_blacklisted,
            cumulative_fee_volume
        )
        SELECT 
            a.address,
            a.name,
            x.id,
            y.id,
            a.bin_step,
            a.base_fee_percentage,
            a.hide,
            a.is_blacklisted,
            a.cumulative_fee_volume
        FROM 
            api_entries a
            JOIN tokens x ON a.mint_x = x.mint
            JOIN tokens y ON a.mint_y = y.mint
        ON CONFLICT DO NOTHING
    ''')

def load_history(conn):
    """
    Loads the historical data into the 'pair_history' table.
    """
    conn.execute('''
        INSERT INTO pair_history (
            created_at,
            pair_id,
            price,
            liquidity,
            fees
        )
        SELECT 
            a.created_at,
            p.id pair_id,
            a.current_price price,
            a.liquidity,
            CAST(a.cumulative_fee_volume AS DOUBLE) - p.cumulative_fee_volume fees
        FROM 
            api_entries a
            JOIN pairs p ON a.address = p.pair_address
    ''')

def update_cumulative_fees(conn, created_at):
    """
    Updates the cumulative fee volume in the 'pairs' table.
    """
    conn.execute('''
        UPDATE pairs 
        SET cumulative_fee_volume = (
            SELECT 
                CAST(cumulative_fee_volume as DOUBLE)
            FROM 
                api_entries
            WHERE 
                address = pairs.pair_address
        )
    ''')