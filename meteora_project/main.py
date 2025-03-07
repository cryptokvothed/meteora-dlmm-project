# main.py

import asyncio
from datetime import datetime, timezone
import logging
import duckdb
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from meteora_project.apis.meteora_dlmm import meteora_lp_api
from meteora_project.db import insert_meteora_api_entries, setup_database
from meteora_project import config

# Configure logging (adjust level as needed)
logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)

async def run_job():
    """Fetch API data, insert it into the database, and log progress."""

    conn = duckdb.connect(config.DB_FILENAME)
    try:
        # Fetch data from the API
        start_time = time.time()
        data = await meteora_lp_api()
        end_time = time.time()
        duration = end_time - start_time
        logger.debug("API call duration: %.2f seconds", duration)

        if not data:
            logger.error("No data fetched from API.")
            return

        # Insert the entries into the database.
        created_at = datetime.now()
        start_time = time.time()
        insert_meteora_api_entries(conn, data, created_at)
        end_time = time.time()
        duration = end_time - start_time
        logger.debug("Time to load API data into database: %.2f seconds", duration)

        conn.close()
        logger.debug("Job complete at %s", datetime.now(timezone.utc).isoformat())

    except Exception as e:
        # Log the exception stack trace for debugging.
        logger.exception("Exception occurred while running the scheduled job: %s", e)

async def load_database():
    # Set up the SQLite database.
    setup_database(config.DB_FILENAME)

    # Run the job immediately on startup
    await run_job()
    logger.debug("Job successfully executed on startup.")

    # Create a blocking scheduler
    scheduler = AsyncIOScheduler()

    # Schedule the job to run every 1 minute
    scheduler.add_job(run_job, 'interval', minutes=1)

    logger.info("Scheduler started; job will run every minute.")
    scheduler.start()

    # Use an asyncio Event to keep the coroutine running
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(load_database())
