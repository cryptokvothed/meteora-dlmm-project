# Meteora API Time Series Collector

This project is a Python application that periodically polls the [Meteora API](https://dlmm-api.meteora.ag/swagger-ui/#/) to collect DLMM data and stores it in a [DuckDB](https://duckdb.org/) database. The application is designed to build a time series of DLMM history.

The motivation behind the project was to better identify DLMM opportunities from analyzing liquidity and fees over time. The 24 Hour Fees / TVL metric is a poor indicator, because liquidity frequently moves in and out of pools. The current liquidity (in the denominator) may not be representative of the actual liquidity that generated the fees (in the numerator).

## Attribution
You are encouraged to use this library to build your own Meteora DLMM community tools. If do you use this library in another project, please be sure to provide attribution to [@kVOTHED](https://x.com/CryptoKvothed) and [@GeekLad](https://x.com/GeekLad).

## Project Structure

```
meteora_project/
├── meteora_project/
│   ├── __init__.py
│   ├── apis/                  
│   │   ├── __init__.py         # Marks the folder as a package.
│   │   └── meteora_dlmm.py     # API module for Meteora DLMM data (formerly api.py).
│   ├── config.py               # Configuration for API endpoints, database filename, and rate limiting settings.
│   └── db.py                   # Functions to set up the DuckDB database and insert API data.
├── main.py                     # Application entry point that schedules API calls at one-minute intervals.
├── tests/                      # (Optional) Folder for tests.
│   └── test_api.py
├── README.md                   # This file.
└── requirements.txt            # List of dependencies.
```

## Features

- **Periodic API Polling:**  
  Uses APScheduler to call the Meteora API every minute.

- **Robust Error Handling:**  
  Integrates Tenacity for retries with exponential backoff and ratelimit to prevent exceeding API rate limits.

- **Time Series Data Collection:**  
  The data from each API call response (with a timestamp) is stored in a DuckDB database for time series analysis.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/cryptokvothed/meteora-dlmm-project.git
   cd meteora-dlmm-project
   ```

2. **Create a Virtual Environment (optional but recommended):**

   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

   Your `requirements.txt` should include:

   ```
   requests
   tenacity
   ratelimit
   apscheduler
   duckdb
   pandas
   ```

## Usage

To start collecting data, simply run:

```bash
python meteora_project/main.py
```

The application will:
- Poll the Meteora API every minute.
- Log API responses and errors.
- Insert the data into a DuckDB database (default file: `api_entries.db`).

Press `Ctrl+C` to stop the scheduler gracefully.

## Configuration

Edit `meteora_project/config.py` to adjust settings such as:
- **API_BASE_URL:** The base URL for the Meteora API.
- **LOG_LEVEL:** The log level.  Set to `logging.DEBUG` for more verbose logging.
- **DB_FILENAME:** The filename for your DuckDB database.
- **CALLS** and **PERIOD:** For rate limiting (e.g., 30 calls per minute).

## License

This project is open source and available under the [MIT License](LICENSE.md).

## Contributing

Feel free to submit issues or pull requests if you have suggestions or improvements.