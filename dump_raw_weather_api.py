"""Utility script to dump raw Open-Meteo API responses to a JSON file."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

import config
from extract import _resolve_city_metadata


def _fetch_raw_payload(city_name: str) -> dict:
    if config.LATITUDE is not None and config.LONGITUDE is not None:
        city_meta = {
            "city_name": city_name,
            "latitude": float(config.LATITUDE),
            "longitude": float(config.LONGITUDE),
            "source": "manual_config_coordinates",
        }
    else:
        geo = _resolve_city_metadata(city_name)
        city_meta = {
            "city_name": geo.get("city_name"),
            "country": geo.get("country"),
            "admin1": geo.get("admin1"),
            "timezone": geo.get("timezone"),
            "latitude": geo["latitude"],
            "longitude": geo["longitude"],
            "source": "geocoding_api",
        }

    params = {
        "latitude": city_meta["latitude"],
        "longitude": city_meta["longitude"],
        "current_weather": "true",
    }

    response = requests.get(config.API_URL, params=params, timeout=30)
    response.raise_for_status()

    return {
        "requested_city": city_name,
        "city_metadata": city_meta,
        "request": {
            "url": config.API_URL,
            "params": params,
        },
        "response": response.json(),
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _default_output_path() -> Path:
    return Path("raw_weather_latest.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump raw Open-Meteo weather API payload.")
    parser.add_argument("--city", default=config.CITY_NAME, help="City name to query")
    parser.add_argument("--output", help="Output JSON file path")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else _default_output_path()
    payload = _fetch_raw_payload(args.city)

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Raw payload saved to: {output_path.resolve()}")


if __name__ == "__main__":
    main()
