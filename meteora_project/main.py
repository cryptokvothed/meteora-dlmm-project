# main.py

from datetime import datetime, timezone
import logging
import sqlite3
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apis.meteora_dlmm import meteora_lp_api
from db import insert_meteora_api_entries, setup_database
import config

# Configure logging (adjust level as needed)
logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)

def run_job():
    """Fetch API data, insert it into the database, and log progress."""

    conn = sqlite3.connect(config.DB_FILENAME)
    try:
        # Fetch data from the API
        start_time = time.time()
        data = meteora_lp_api()
        end_time = time.time()
        duration = end_time - start_time
        logger.debug("API call duration: %.2f seconds", duration)

        if not data:
            logger.error("No data fetched from API.")
            return

        # Expecting a structure with a "pairs" list and a "total" count.
        if isinstance(data, dict) and "pairs" in data:
            entries = data["pairs"]
            total_entries = data.get("total", len(entries))
            logger.debug("Total entries reported by API: %s", total_entries)
        else:
            entries = data

        # Insert the entries into the database.
        created_at = datetime.now(timezone.utc).timestamp()  # Convert to unix
        start_time = time.time()
        insert_meteora_api_entries(conn, entries, created_at)
        end_time = time.time()
        duration = end_time - start_time
        logger.debug("Time to load API data into database: %.2f seconds", duration)

        conn.close()
        logger.debug("Job complete at %s", datetime.now(timezone.utc).isoformat())

    except Exception as e:
        # Log the exception stack trace for debugging.
        logger.exception("Exception occurred while running the scheduled job: %s", e)

if __name__ == "__main__":
    # Set up the SQLite database.
    setup_database(config.DB_FILENAME)

    # Run the job immediately on startup
    run_job()
    logger.info("Job successfully executed on startup.")

    # Create a blocking scheduler
    scheduler = BlockingScheduler()

    # Schedule the job to run every 1 minute
    scheduler.add_job(run_job, 'interval', minutes=1)

    logger.info("Scheduler started; job will run every minute.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
