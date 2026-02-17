"""Transform step: clean and enrich weather data."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict

import pandas as pd


logger = logging.getLogger(__name__)


def transform_weather_data(raw_data: Dict[str, Any], city_name: str) -> pd.DataFrame:
    """Transform extracted weather payload into DB-ready schema."""
    df = pd.DataFrame([raw_data])

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if df["timestamp"].isna().any():
        raise ValueError("Invalid or missing timestamp in extracted data")

    df.rename(columns={"temperature": "temp_celsius"}, inplace=True)

    df["temp_f"] = (df["temp_celsius"] * 9.0 / 5.0) + 32.0

    numeric_defaults = {
        "temp_celsius": 0.0,
        "temp_f": 32.0,
        "windspeed": 0.0,
    }
    df = df.fillna(value=numeric_defaults)

    df["temp_celsius"] = df["temp_celsius"].astype(float)
    df["temp_f"] = df["temp_f"].astype(float)
    df["windspeed"] = df["windspeed"].astype(float)

    if "city_name" in df.columns:
        df["city_name"] = df["city_name"].fillna(city_name)
    else:
        df["city_name"] = city_name

    if "country" not in df.columns:
        df["country"] = None
    if "admin1" not in df.columns:
        df["admin1"] = None

    if "lat" in df.columns:
        df["lat"] = df["lat"].fillna(0.0).astype(float)
    elif "latitude" in df.columns:
        df["lat"] = df["latitude"].fillna(0.0).astype(float)
    else:
        df["lat"] = 0.0

    if "lon" in df.columns:
        df["lon"] = df["lon"].fillna(0.0).astype(float)
    elif "longi" in df.columns:
        df["lon"] = df["longi"].fillna(0.0).astype(float)
    elif "longitude" in df.columns:
        df["lon"] = df["longitude"].fillna(0.0).astype(float)
    else:
        df["lon"] = 0.0
    df["data_collected_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    df = df[
        [
            "city_name",
            "country",
            "admin1",
            "lat",
            "lon",
            "timestamp",
            "temp_celsius",
            "temp_f",
            "windspeed",
            "data_collected_at",
        ]
    ]

    logger.info("Data transformed")
    return df
