"""Configuration for the Weather ETL pipeline."""

import os
from pathlib import Path


def _load_dotenv() -> None:
    """Load environment variables from .env files if present."""
    env_paths = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]

    for env_path in env_paths:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


_load_dotenv()

API_URL = "https://api.open-meteo.com/v1/forecast"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"

CITY_NAME = os.getenv("CITY_NAME", "Vijayawada")

# Optional manual override. If unset, coordinates are resolved from city name.
LATITUDE = os.getenv("LATITUDE")
LONGITUDE = os.getenv("LONGITUDE")

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DATABASE", "weather_db")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))

LOCATION_TABLE = "locations"
WEATHER_TABLE = "weather_data"
