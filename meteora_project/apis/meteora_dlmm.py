# meteora_dlmm.py

import requests
import json
from datetime import datetime, timezone
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
import config

logger = logging.getLogger(__name__)

# Retrieve the specific rate limit for the Meteora DLMM API
meteora_rate = config.RATE_LIMITS.get("meteora_dlmm", {"calls": 30, "period": 60})
calls = meteora_rate["calls"]
period = meteora_rate["period"]

@sleep_and_retry
@limits(calls=calls, period=period)
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def meteora_lp_api(page=1, limit=config.DEFAULT_LIMIT, order_by='desc', skip_size=0, sort_key='volume'):
    """
    Calls the Meteora API and returns the JSON response along with a timezone-aware API return timestamp.
    """
    base_url = config.API_BASE_URL
    endpoint = "/pair/all_with_pagination"
    params = {
        "page": page,
        "limit": limit,
        "order_by": order_by,
        "skip_size": skip_size,
        "sort_key": sort_key,
    }
    
    response = requests.get(base_url + endpoint, params=params, timeout=10)
    response.raise_for_status()  # Will trigger retry if status is not 200
    
    data = response.json()
    api_timestamp = datetime.now(timezone.utc).isoformat()
    logger.debug("API Response Timestamp: %s", api_timestamp)
    logger.debug("API Response: %s", json.dumps(data, indent=4))
    return data, api_timestamp
