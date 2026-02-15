"""Extract step: fetch weather data from Open-Meteo."""

from __future__ import annotations

import logging
from typing import Any, Dict

import requests

import config


logger = logging.getLogger(__name__)


def _resolve_city_metadata(city_name: str) -> Dict[str, Any]:
    """Resolve city metadata using Open-Meteo geocoding API."""
    params = {"name": city_name, "count": 1}

    try:
        response = requests.get(config.GEOCODING_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.exception("Geocoding API request failed")
        raise RuntimeError("Failed to resolve coordinates from city name") from exc

    results = payload.get("results") or []
    if not results:
        raise RuntimeError(f"No geocoding result found for city '{city_name}'")

    top = results[0]
    latitude = top.get("latitude")
    longitude = top.get("longitude")
    if latitude is None or longitude is None:
        raise RuntimeError("Geocoding response missing latitude/longitude")

    return {
        "city_name": top.get("name") or city_name,
        "country": top.get("country"),
        "admin1": top.get("admin1"),
        "timezone": top.get("timezone"),
        "latitude": float(latitude),
        "longitude": float(longitude),
    }


def extract_weather_data(city_name: str | None = None) -> Dict[str, Any]:
    """Fetch current weather data for the configured location."""
    resolved_city = city_name or config.CITY_NAME

    if config.LATITUDE is not None and config.LONGITUDE is not None:
        city_meta = {
            "city_name": resolved_city,
            "country": None,
            "admin1": None,
            "timezone": None,
            "latitude": float(config.LATITUDE),
            "longitude": float(config.LONGITUDE),
        }
    else:
        city_meta = _resolve_city_metadata(resolved_city)

    params = {
        "latitude": city_meta["latitude"],
        "longitude": city_meta["longitude"],
        "current_weather": "true",
    }

    try:
        response = requests.get(config.API_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.exception("API request failed")
        raise RuntimeError("Failed to fetch weather data from Open-Meteo") from exc

    current_weather = payload.get("current_weather")
    if not current_weather:
        raise RuntimeError("Open-Meteo response did not contain current_weather")

    extracted = {
        "city_name": city_meta["city_name"],
        "country": city_meta["country"],
        "admin1": city_meta["admin1"],
        "timezone": city_meta["timezone"],
        "latitude": city_meta["latitude"],
        "longitude": city_meta["longitude"],
        "timestamp": current_weather.get("time"),
        "temperature": current_weather.get("temperature"),
        "windspeed": current_weather.get("windspeed"),
    }

    logger.info("Data extracted")
    return extracted
