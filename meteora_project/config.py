import logging

# config.py

# API Configuration
API_BASE_URL = "https://dlmm-api.meteora.ag"
DEFAULT_LIMIT = 100
LOG_LEVEL = logging.INFO  # Set to logging.DEBUG for more verbose output

# Database configuration
DB_FILENAME = "api_entries.db"

# Rate Limiting Configuration
# Each API can have its own "calls" (number of allowed calls) and "period" (in seconds)
RATE_LIMITS = {
    "meteora_dlmm": {
        "calls": 30,  # 30 calls
        "period": 60  # per 60 seconds (i.e., 30 calls per minute)
    },
    "another_api": {
        "calls": 10,  # 10 calls
        "period": 60  # per 60 seconds (i.e., 10 calls per minute)
    },
    # Add additional APIs here with their unique rate limits
}
