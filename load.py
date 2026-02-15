"""Load step: write transformed weather data into MySQL."""

from __future__ import annotations

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
    """Create the target database and table if they do not exist."""
    try:
        conn = _connect(with_database=False)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{config.DB_NAME}`")
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while creating/checking database")
        raise RuntimeError("MySQL database setup failed") from exc

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{config.TABLE_NAME}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city_name VARCHAR(100) NOT NULL,
        country VARCHAR(100),
        admin1 VARCHAR(100),
        lat DOUBLE,
        longi DOUBLE,
        `timestamp` DATETIME NOT NULL,
        temp_celsius FLOAT,
        temp_f FLOAT,
        windspeed FLOAT,
        data_collected_at DATETIME NOT NULL,
        UNIQUE KEY uq_city_timestamp (city_name, `timestamp`)
    )
    """

    required_columns = {
        "country": "VARCHAR(100)",
        "admin1": "VARCHAR(100)",
        "lat": "DOUBLE",
        "longi": "DOUBLE",
        "temp_celsius": "FLOAT",
        "temp_f": "FLOAT",
    }

    try:
        conn = _connect(with_database=True)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (config.DB_NAME, config.TABLE_NAME),
        )
        existing_columns = {row[0] for row in cursor.fetchall()}
        for column_name, column_type in required_columns.items():
            if column_name not in existing_columns:
                cursor.execute(
                    f"ALTER TABLE `{config.TABLE_NAME}` ADD COLUMN `{column_name}` {column_type}"
                )
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while creating/checking table")
        raise RuntimeError("MySQL table setup failed") from exc


def load_weather_data(df) -> int:
    """Insert transformed records into MySQL in an idempotent way."""
    expected_columns = [
        "city_name",
        "timestamp",
        "temp_celsius",
        "temp_f",
        "windspeed",
        "data_collected_at",
        "country",
        "admin1",
        "lat",
        "longi",
    ]
    df = df[expected_columns]

    insert_sql = f"""
    INSERT INTO `{config.TABLE_NAME}`
    (city_name, `timestamp`, temp_celsius, temp_f, windspeed, data_collected_at, country, admin1, lat, longi)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        temp_celsius = VALUES(temp_celsius),
        temp_f = VALUES(temp_f),
        windspeed = VALUES(windspeed),
        data_collected_at = VALUES(data_collected_at),
        country = VALUES(country),
        admin1 = VALUES(admin1),
        lat = VALUES(lat),
        longi = VALUES(longi)
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
        cursor.executemany(insert_sql, records)
        conn.commit()
        rowcount = cursor.rowcount
        cursor.close()
        conn.close()
    except mysql.connector.Error as exc:
        logger.exception("Failed while loading data into MySQL")
        raise RuntimeError("MySQL insert failed") from exc

    logger.info("Data loaded")
    return rowcount
