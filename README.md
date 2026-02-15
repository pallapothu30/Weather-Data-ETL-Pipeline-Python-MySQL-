# Weather Data ETL Pipeline (Python + MySQL)

A modular ETL project that extracts current weather from Open-Meteo, transforms it with pandas, and loads it into a local MySQL table.

## Project Structure

```text
weather_etl/
|-- extract.py
|-- transform.py
|-- load.py
|-- main.py
|-- config.py
|-- requirements.txt
`-- README.md
```

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Update DB credentials in `config.py` or set environment variables:

- `MYSQL_HOST`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_PORT`

4. Set location input (optional env vars):

- `CITY_NAME` (default: `Vijayawada`)
- `COUNTRY_CODE` (default: `IN`)
- `LATITUDE` and `LONGITUDE` (optional manual override)

## MySQL Target

- Database: `weather_db`
- Table: `weather_data`
- Duplicate handling: unique key on (`city_name`, `timestamp`) and `ON DUPLICATE KEY UPDATE`
- Extra city metadata persisted per row: `country`, `country_code`, `admin1`, `timezone`, `latitude`, `longitude`

## Run

From inside `weather_etl/`:

```bash
python main.py
```

Run with city flag:

```bash
python main.py --city "Pune"
```

Expected logs:

- `Data extracted`
- `Data transformed`
- `Data loaded`
- `Pipeline completed successfully...`

## Notes

- API source: Open-Meteo current weather endpoint.
- By default, coordinates are auto-resolved from city name via Open-Meteo geocoding.
- The pipeline inserts one current-weather record per run.
