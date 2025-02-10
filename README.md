```markdown
# Meteora API Time Series Collector

This project is a Python application that periodically polls the Meteora API to collect data and stores it into a SQLite database. The application is designed to build a time series of API responses for further analysis.

## Project Structure

```
meteora_project/
├── api.py         # Contains functions to call the Meteora API with error handling, retries, and rate limiting.
├── config.py      # Configuration for API endpoints, database filename, and rate limiting settings.
├── db.py          # Functions to set up the SQLite database and insert API data.
├── main.py        # Main application file that schedules API calls (using APScheduler) at one-minute intervals.
└── README.md      # This file.
```

## Features

- **Periodic API Polling:**  
  Uses APScheduler to call the Meteora API every minute.

- **Robust Error Handling:**  
  Integrates Tenacity for retries with exponential backoff, and ratelimit to prevent exceeding API rate limits.

- **Time Series Data Collection:**  
  Each API call’s response (with a timestamp) is stored in a SQLite database for time series analysis.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/meteora-project.git
   cd meteora-project
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
   ```

## Usage

To start collecting data, simply run:

```bash
python main.py
```

The application will:
- Poll the Meteora API every minute.
- Log API responses and errors.
- Insert the data into a SQLite database (default file: `api_entries.db`).

Press `Ctrl+C` to stop the scheduler gracefully.

## Configuration

Edit `config.py` to adjust settings such as:
- `API_BASE_URL`: The base URL for the Meteora API.
- `DEFAULT_LIMIT`: The number of records to request from the API.
- `DB_FILENAME`: The filename for your SQLite database.
- `CALLS` and `PERIOD`: For rate limiting (e.g., 30 calls per minute).

## License

This project is open source and available under the [MIT License](LICENSE).

## Contributing

Feel free to submit issues or pull requests if you have suggestions or improvements.
```