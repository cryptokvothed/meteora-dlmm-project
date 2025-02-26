import logging
import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://dlmm-api.meteora.ag")

# Logging Configuration
LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)

# Default Limit
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", 100))

# Database configuration
DB_FILENAME = os.getenv("DB_FILENAME", "meteora_dlmm_time_series.duckdb")

# Rate Limiting Configuration
RATE_LIMITS = {
    "meteora_dlmm": {
        "calls": int(os.getenv("RATE_LIMIT_CALLS", 3)),
        "period": int(os.getenv("RATE_LIMIT_PERIOD", 1))
    }
}
