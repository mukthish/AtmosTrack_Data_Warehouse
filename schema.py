"""
BigQuery table schema and Kafka-JSON → BigQuery field-name mapping.

The Kafka messages use camelCase (Java Reading model).
BigQuery columns use snake_case.
"""

from google.cloud import bigquery

# ── camelCase (Kafka JSON) → snake_case (BigQuery) ─────────────────
FIELD_MAP = {
    "city":              "city",
    "hotspot":           "hotspot",
    "timestamp":         "timestamp",
    "aqi":               "aqi",
    "pm25":              "pm25",
    "pm10":              "pm10",
    "no":                "no",
    "no2":               "no2",
    "nox":               "nox",
    "nh3":               "nh3",
    "so2":               "so2",
    "co":                "co",
    "ozone":             "ozone",
    "benzene":           "benzene",
    "toluene":           "toluene",
    "xylene":            "xylene",
    "oXylene":           "o_xylene",
    "ethBenzene":        "eth_benzene",
    "mpXylene":          "mp_xylene",
    "temperature":       "temperature",
    "humidity":          "humidity",
    "windSpeed":         "wind_speed",
    "windDirection":     "wind_direction",
    "rainfall":          "rainfall",
    "totalRainfall":     "total_rainfall",
    "solarRadiation":    "solar_radiation",
    "pressure":          "pressure",
    "verticalWindSpeed": "vertical_wind_speed",
    "latitude":          "latitude",
    "longitude":         "longitude",
}

# ── BigQuery table schema ──────────────────────────────────────────
TABLE_SCHEMA = [
    # Identity
    bigquery.SchemaField("city",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("hotspot",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),

    # AQI
    bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),

    # Core pollutants
    bigquery.SchemaField("pm25",  "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("pm10",  "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("no",    "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("no2",   "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("nox",   "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("nh3",   "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("so2",   "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("co",    "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ozone", "FLOAT", mode="NULLABLE"),

    # Organic pollutants
    bigquery.SchemaField("benzene",      "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("toluene",      "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("xylene",       "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("o_xylene",     "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("eth_benzene",  "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("mp_xylene",    "FLOAT", mode="NULLABLE"),

    # Weather
    bigquery.SchemaField("temperature",        "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("humidity",           "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("wind_speed",         "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("wind_direction",     "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rainfall",           "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_rainfall",     "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("solar_radiation",    "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("pressure",           "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("vertical_wind_speed","FLOAT", mode="NULLABLE"),

    # Geo
    bigquery.SchemaField("latitude",  "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),

    # Audit
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
]


def map_record(raw: dict) -> dict:
    """Convert a camelCase Kafka JSON dict to a snake_case BigQuery row."""
    row = {}
    for kafka_key, bq_key in FIELD_MAP.items():
        if kafka_key in raw:
            row[bq_key] = raw[kafka_key]
    return row
