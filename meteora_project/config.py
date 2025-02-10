# config.py

# API configuration
API_BASE_URL = "https://dlmm-api.meteora.ag"
DEFAULT_LIMIT = 100

# Database configuration
DB_FILENAME = "api_entries.db"

# Rate limiting configuration (for ratelimit)
CALLS = 30           # maximum calls
PERIOD = 60          # per period in seconds (i.e. 30 calls per minute)
