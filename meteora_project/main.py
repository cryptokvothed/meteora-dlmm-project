# main.py

import json
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from meteora_project.apis.meteora_dlmm import meteora_lp_api
from meteora_project.db import setup_database, insert_api_entry
from meteora_project import config

# Configure logging (adjust level as needed)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_job():
    """Fetch API data, insert it into the database, and log progress."""
    try:
        # Fetch data from the API along with the API return timestamp.
        data, api_timestamp = meteora_lp_api()
        if not data:
            logger.error("No data fetched from API.")
            return

        # Expecting a structure with a "pairs" list and a "total" count.
        if isinstance(data, dict) and "pairs" in data:
            entries = data["pairs"]
            total_entries = data.get("total", len(entries))
            logger.info("Total entries reported by API: %s", total_entries)
        else:
            entries = data

        # Set up the SQLite database.
        conn = setup_database(config.DB_FILENAME)

        # Insert each entry into the database.
        for entry in entries:
            logger.info("Inserting entry: %s", json.dumps(entry, indent=4))
            insert_api_entry(conn, entry, api_timestamp)

        conn.close()
        logger.info("Job complete at %s", api_timestamp)

    except Exception as e:
        # Log the exception stack trace for debugging.
        logger.exception("Exception occurred while running the scheduled job: %s", e)

if __name__ == "__main__":
    # Create a blocking scheduler
    scheduler = BlockingScheduler()

    # Schedule the job to run every 1 minute
    scheduler.add_job(run_job, 'interval', minutes=1)

    logger.info("Scheduler started; job will run every minute.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
