import requests
import json
import asyncio
import aiohttp
from datetime import datetime, timezone
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
from meteora_project import config

logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)

# Retrieve the specific rate limit for the Meteora DLMM API
meteora_rate = config.RATE_LIMITS.get("meteora_dlmm", {"calls": 3, "period": 1})
calls = meteora_rate["calls"]
period = meteora_rate["period"]

@sleep_and_retry
@limits(calls=calls, period=period)
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def get_organic_score(token_ca):
    """
    Get the organic score for a token
    """
    endpoint = f"/tokens/search?query={token_ca}"
    url = config.JUPITER_API_BASE_URL + endpoint
    async with aiohttp.ClientSession() as session:
      async with session.get(url, timeout=10) as response:
        response.raise_for_status()  # Will trigger retry if status is not 200
        result = await response.json()
        try:
          return round(float(result["tokens"][0]["organicScore"]))
        except:
          return None