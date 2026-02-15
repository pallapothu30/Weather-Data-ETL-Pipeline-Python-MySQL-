"""Main entrypoint for the Weather ETL pipeline."""

from __future__ import annotations

import argparse
import logging

import config
from extract import extract_weather_data
from load import ensure_database_and_table, load_weather_data
from transform import transform_weather_data


def run_pipeline(city_name: str | None = None) -> None:
    """Run extract, transform, and load steps in sequence."""
    resolved_city = city_name or config.CITY_NAME
    raw_data = extract_weather_data(city_name=resolved_city)
    transformed_df = transform_weather_data(raw_data, city_name=resolved_city)
    ensure_database_and_table()
    inserted = load_weather_data(transformed_df)
    logging.info("Pipeline completed successfully. Rows affected: %s", inserted)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run weather ETL pipeline.")
    parser.add_argument("--city", help="City name to fetch weather for (e.g., Pune)")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        args = _parse_args()
        run_pipeline(city_name=args.city)
    except Exception as exc:
        logging.exception("Pipeline failed: %s", exc)
        raise
