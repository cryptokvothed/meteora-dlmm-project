# api.py

import requests
import json
from datetime import datetime, timezone
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
import config

logger = logging.getLogger(__name__)

@sleep_and_retry
@limits(calls=config.CALLS, period=config.PERIOD)
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def meteora_lp_api(page=1, limit=config.DEFAULT_LIMIT, order_by='desc', skip_size=0, sort_key='volume'):
    """
    Calls the Meteora API and returns the JSON response along with a timezone-aware API return timestamp.
    
    This function is decorated to:
      - Enforce a rate limit,
      - Retry up to 5 times on exceptions with exponential backoff,
      - Use a timeout on the HTTP request.
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
    # This will raise an HTTPError for non-200 responses and trigger a retry.
    response.raise_for_status()
    
    data = response.json()
    api_timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("API Response Timestamp: %s", api_timestamp)
    logger.debug("API Response: %s", json.dumps(data, indent=4))
    return data, api_timestamp
