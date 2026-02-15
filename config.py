"""Configuration for the Weather ETL pipeline."""

import os

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

TABLE_NAME = "weather_data"
