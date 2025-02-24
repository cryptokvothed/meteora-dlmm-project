import requests
import json
import asyncio
import aiohttp
from datetime import datetime, timezone
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
from meteora_project import config

# Retrieve the specific rate limit for the Meteora DLMM API
meteora_rate = config.RATE_LIMITS.get("meteora_dlmm", {"calls": 3, "period": 1})
calls = meteora_rate["calls"]
period = meteora_rate["period"]

@sleep_and_retry
@limits(calls=calls, period=period)
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_page_data(session, page, limit, sort_key):
    """
    Helper function to fetch a single page of data from the Meteora API.
    """
    endpoint = f"/pair/all_with_pagination?page={page}&limit={limit}&sort_key={sort_key}"
    url = config.API_BASE_URL + endpoint
    async with session.get(url, timeout=10) as response:
        response.raise_for_status()  # Will trigger retry if status is not 200
        return await response.json()

async def fetch_paginated_data(limit=config.DEFAULT_LIMIT, sort_key="volume30m"):
    """
    Handles pagination, looping through pages and aggregating results from the Meteora API.
    """
    page = 0
    results = []

    async with aiohttp.ClientSession() as session:
        while True:
            # Fetch data for the current page
            data = await fetch_page_data(session, page, limit, sort_key)
            
            # Collect the pairs from the response
            pairs = data.get('pairs', [])
            results.extend(pairs)
            
            # Check for the stopping condition: if any pair has volume.min_30 == 0
            if any(pair['fees']['min_30'] == 0 for pair in pairs):
                print("Found pair with zero volume in the last 30 minutes. Stopping.")
                break
            
            # Update the page number for pagination
            page += 1

            # Sleep to respect the rate limit if necessary
            await asyncio.sleep(period)

    return results

async def meteora_lp_api(limit=config.DEFAULT_LIMIT, sort_key="volume30m"):
    """
    Calls the Meteora API, handles rate limits, pagination, and returns aggregated data.
    The function delegates pagination logic to fetch_paginated_data.
    """
    # Start the data aggregation process using pagination
    results = await fetch_paginated_data(limit=limit, sort_key=sort_key)

    # Save the aggregated results to a JSON file
    with open('aggregated_data.json', 'w') as json_file:
        json.dump(results, json_file, indent=4)

    print("Data aggregation complete. Results saved to 'aggregated_data.json'.")
    return results