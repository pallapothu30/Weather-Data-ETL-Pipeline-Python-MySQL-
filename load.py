"""Load step: write transformed weather data into MySQL."""

from __future__ import annotations

from datetime import datetime
import logging

import mysql.connector
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor

import config


logger = logging.getLogger(__name__)


def _connect(with_database: bool) -> MySQLConnection:
    params = {
        "host": config.DB_HOST,
        "user": config.DB_USER,
        "password": config.DB_PASSWORD,
        "port": config.DB_PORT,
    }
    if with_database:
        params["database"] = config.DB_NAME

    return mysql.connector.connect(**params)


def ensure_database_and_table() -> None:
    """Create normalized target tables if they do not exist."""
    try:
        conn = _connect(with_database=False)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{config.DB_NAME}`")
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while creating/checking database")
        raise RuntimeError("MySQL database setup failed") from exc

    try:
        conn = _connect(with_database=True)
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{config.LOCATION_TABLE}` (
                location_id INT AUTO_INCREMENT PRIMARY KEY,
                city_name VARCHAR(100) NOT NULL,
                country VARCHAR(100),
                admin1 VARCHAR(100),
                lat DOUBLE,
                lon DOUBLE,
                UNIQUE KEY uq_city_coords (city_name, lat, lon)
            )
            """
        )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (config.DB_NAME, config.WEATHER_TABLE),
        )
        weather_exists = cursor.fetchone()[0] > 0

        if weather_exists:
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (config.DB_NAME, config.WEATHER_TABLE),
            )
            existing_weather_cols = {row[0] for row in cursor.fetchall()}
            if "location_id" not in existing_weather_cols:
                backup_name = f"{config.WEATHER_TABLE}_legacy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                cursor.execute(
                    f"RENAME TABLE `{config.WEATHER_TABLE}` TO `{backup_name}`"
                )
                logger.warning(
                    "Renamed legacy denormalized table to `%s` before creating normalized schema",
                    backup_name,
                )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{config.WEATHER_TABLE}` (
                weather_id INT AUTO_INCREMENT PRIMARY KEY,
                location_id INT NOT NULL,
                `timestamp` DATETIME NOT NULL,
                temp_celsius FLOAT,
                temp_f FLOAT,
                windspeed FLOAT,
                data_collected_at DATETIME NOT NULL,
                FOREIGN KEY (location_id)
                    REFERENCES `{config.LOCATION_TABLE}`(location_id),
                UNIQUE KEY uq_loc_time (location_id, `timestamp`)
            )
            """
        )
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (config.DB_NAME, config.WEATHER_TABLE),
        )
        existing_weather_cols = {row[0] for row in cursor.fetchall()}
        if "location_id" not in existing_weather_cols:
            raise RuntimeError(
                f"Table `{config.WEATHER_TABLE}` exists but is not normalized. "
                "Expected `location_id` column."
            )
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while creating/checking table")
        raise RuntimeError("MySQL table setup failed") from exc


def load_weather_data(df) -> int:
    """Insert transformed records into normalized MySQL tables."""
    expected_columns = [
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
    if "lon" not in df.columns and "longi" in df.columns:
        df = df.rename(columns={"longi": "lon"})
    df = df[expected_columns]

    upsert_location_sql = f"""
    INSERT INTO `{config.LOCATION_TABLE}` (city_name, country, admin1, lat, lon)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        country = VALUES(country),
        admin1 = VALUES(admin1),
        location_id = LAST_INSERT_ID(location_id)
    """

    upsert_weather_sql = f"""
    INSERT INTO `{config.WEATHER_TABLE}`
    (location_id, `timestamp`, temp_celsius, temp_f, windspeed, data_collected_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        temp_celsius = VALUES(temp_celsius),
        temp_f = VALUES(temp_f),
        windspeed = VALUES(windspeed),
        data_collected_at = VALUES(data_collected_at)
    """

    records = []
    for row in df.itertuples(index=False, name=None):
        normalized_row = []
        for value in row:
            if hasattr(value, "to_pydatetime"):
                normalized_row.append(value.to_pydatetime())
            else:
                normalized_row.append(value)
        records.append(tuple(normalized_row))

    try:
        conn = _connect(with_database=True)
        cursor: MySQLCursor = conn.cursor()
        weather_records = []
        for row in records:
            (
                city_name,
                country,
                admin1,
                lat,
                lon,
                timestamp,
                temp_celsius,
                temp_f,
                windspeed,
                data_collected_at,
            ) = row

            cursor.execute(upsert_location_sql, (city_name, country, admin1, lat, lon))
            location_id = cursor.lastrowid
            weather_records.append(
                (
                    location_id,
                    timestamp,
                    temp_celsius,
                    temp_f,
                    windspeed,
                    data_collected_at,
                )
            )

        cursor.executemany(upsert_weather_sql, weather_records)
        conn.commit()
        rowcount = cursor.rowcount
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while loading data into MySQL")
        raise RuntimeError("MySQL insert failed") from exc

    logger.info("Data loaded")
    return rowcount
