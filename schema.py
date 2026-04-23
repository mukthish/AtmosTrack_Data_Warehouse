from __future__ import annotations

"""
Star-schema definitions and helpers for the AtmosTrack analytics warehouse.

Source rows are hourly aggregates with city/hotspot identity and a set of
pollutant + weather measures. This module converts those rows into:
- dim_location
- dim_time
- fact_air_quality_hourly
"""

import hashlib
from datetime import datetime, timezone

from google.cloud import bigquery

# camelCase (Kafka JSON) -> snake_case (warehouse)
FIELD_MAP = {
    "city": "city",
    "hotspot": "hotspot",
    "timestamp": "timestamp",
    "aqi": "aqi",
    "pm25": "pm25",
    "pm10": "pm10",
    "no": "no",
    "no2": "no2",
    "nox": "nox",
    "nh3": "nh3",
    "so2": "so2",
    "co": "co",
    "ozone": "ozone",
    "benzene": "benzene",
    "toluene": "toluene",
    "xylene": "xylene",
    "oXylene": "o_xylene",
    "ethBenzene": "eth_benzene",
    "mpXylene": "mp_xylene",
    "temperature": "temperature",
    "humidity": "humidity",
    "windSpeed": "wind_speed",
    "windDirection": "wind_direction",
    "rainfall": "rainfall",
    "totalRainfall": "total_rainfall",
    "solarRadiation": "solar_radiation",
    "pressure": "pressure",
    "verticalWindSpeed": "vertical_wind_speed",
    "latitude": "latitude",
    "longitude": "longitude",
}

# Dimension table schemas
DIM_LOCATION_SCHEMA = [
    bigquery.SchemaField("location_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hotspot", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
]

DIM_TIME_SCHEMA = [
    bigquery.SchemaField("time_key", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("timestamp_hour", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("calendar_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("quarter", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("hour", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("day_of_week", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("is_weekend", "BOOLEAN", mode="REQUIRED"),
]

# Fact table schema
FACT_AIR_QUALITY_HOURLY_SCHEMA = [
    bigquery.SchemaField("fact_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("location_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("time_key", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("pm25", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("pm10", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("no", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("no2", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("nox", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("nh3", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("so2", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("co", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ozone", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("benzene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("toluene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("xylene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("o_xylene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("eth_benzene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("mp_xylene", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("temperature", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("humidity", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("wind_direction", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rainfall", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_rainfall", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("solar_radiation", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("pressure", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("vertical_wind_speed", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
]

MEASURE_FIELDS = [
    "aqi",
    "pm25",
    "pm10",
    "no",
    "no2",
    "nox",
    "nh3",
    "so2",
    "co",
    "ozone",
    "benzene",
    "toluene",
    "xylene",
    "o_xylene",
    "eth_benzene",
    "mp_xylene",
    "temperature",
    "humidity",
    "wind_speed",
    "wind_direction",
    "rainfall",
    "total_rainfall",
    "solar_radiation",
    "pressure",
    "vertical_wind_speed",
]


def map_record(raw: dict) -> dict:
    """Convert a camelCase Kafka JSON dict to a warehouse row."""
    row = {}
    for kafka_key, dw_key in FIELD_MAP.items():
        if kafka_key in raw:
            row[dw_key] = raw[kafka_key]
    return row


def _normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_timestamp(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("timestamp is required for star-schema rows")

    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
    elif isinstance(value, datetime):
        dt = value
    else:
        raise TypeError(f"unsupported timestamp type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def make_location_key(city: str | None, hotspot: str | None) -> str:
    base = f"{_normalize_text(city)}|{_normalize_text(hotspot)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def make_time_key(ts: datetime) -> int:
    return int(ts.strftime("%Y%m%d%H%M"))


def make_fact_id(location_key: str, ts: datetime) -> str:
    base = f"{location_key}|{ts.isoformat()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_location_dim_row(raw_row: dict) -> dict:
    city = raw_row.get("city")
    hotspot = raw_row.get("hotspot")
    return {
        "location_key": make_location_key(city, hotspot),
        "city": city,
        "hotspot": hotspot,
        "latitude": raw_row.get("latitude"),
        "longitude": raw_row.get("longitude"),
    }


def build_time_dim_row(ts: datetime) -> dict:
    ts_utc = _normalize_timestamp(ts)
    return {
        "time_key": make_time_key(ts_utc),
        "timestamp_hour": ts_utc.isoformat(),
        "calendar_date": ts_utc.date().isoformat(),
        "year": ts_utc.year,
        "quarter": ((ts_utc.month - 1) // 3) + 1,
        "month": ts_utc.month,
        "day": ts_utc.day,
        "hour": ts_utc.hour,
        "day_of_week": ts_utc.isoweekday(),
        "is_weekend": ts_utc.isoweekday() in (6, 7),
    }


def build_fact_row(raw_row: dict) -> dict:
    ts = _normalize_timestamp(raw_row.get("timestamp"))
    location_key = make_location_key(raw_row.get("city"), raw_row.get("hotspot"))

    fact_row = {
        "fact_id": make_fact_id(location_key, ts),
        "location_key": location_key,
        "time_key": make_time_key(ts),
        "timestamp": ts.isoformat(),
        "ingested_at": raw_row.get("ingested_at"),
    }

    for field in MEASURE_FIELDS:
        fact_row[field] = raw_row.get(field)

    return fact_row


def to_star_rows(raw_row: dict) -> tuple[dict, dict, dict]:
    ts = _normalize_timestamp(raw_row.get("timestamp"))
    return (
        build_location_dim_row(raw_row),
        build_time_dim_row(ts),
        build_fact_row(raw_row),
    )
