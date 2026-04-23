"""
BigQuery batch loader for star-schema analytics tables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

import config
from schema import (
    DIM_LOCATION_SCHEMA,
    DIM_TIME_SCHEMA,
    FACT_AIR_QUALITY_HOURLY_SCHEMA,
    to_star_rows,
)

log = logging.getLogger(__name__)


class BigQueryLoader:
    """Manages BigQuery client, star-schema table creation, and batch merges."""

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            config.SERVICE_ACCOUNT_KEY,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        self._client = bigquery.Client(
            project=config.GCP_PROJECT_ID,
            credentials=credentials,
        )

        self._dataset = config.BQ_DATASET
        self._dataset_ref = bigquery.DatasetReference(config.GCP_PROJECT_ID, self._dataset)

        self._dim_location_table_name = getattr(config, "BQ_DIM_LOCATION_TABLE", "dim_location")
        self._dim_time_table_name = getattr(config, "BQ_DIM_TIME_TABLE", "dim_time")
        self._fact_table_name = getattr(config, "BQ_FACT_AIR_QUALITY_TABLE", "fact_air_quality_hourly")

        self._dim_location_table_id = (
            f"{config.GCP_PROJECT_ID}.{self._dataset}.{self._dim_location_table_name}"
        )
        self._dim_time_table_id = (
            f"{config.GCP_PROJECT_ID}.{self._dataset}.{self._dim_time_table_name}"
        )
        self._fact_table_id = f"{config.GCP_PROJECT_ID}.{self._dataset}.{self._fact_table_name}"
        self._dml_merge_enabled = True

        self._ensure_dataset_and_tables()

    def _ensure_dataset_and_tables(self) -> None:
        """Create the dataset and star-schema tables if they don't already exist."""
        try:
            self._client.get_dataset(self._dataset_ref)
            log.info("Dataset '%s' already exists.", self._dataset)
        except Exception:
            dataset = bigquery.Dataset(self._dataset_ref)
            dataset.location = "US"
            self._client.create_dataset(dataset, exists_ok=True)
            log.info("Created dataset '%s'.", self._dataset)

        self._ensure_table(self._dim_location_table_name, DIM_LOCATION_SCHEMA)
        self._ensure_table(self._dim_time_table_name, DIM_TIME_SCHEMA)
        self._ensure_table(self._fact_table_name, FACT_AIR_QUALITY_HOURLY_SCHEMA)

    def _ensure_table(self, table_name: str, schema: list[bigquery.SchemaField]) -> None:
        table_ref = self._dataset_ref.table(table_name)
        table_id = f"{config.GCP_PROJECT_ID}.{self._dataset}.{table_name}"

        try:
            self._client.get_table(table_ref)
            log.info("Table '%s' already exists.", table_id)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self._client.create_table(table, exists_ok=True)
            log.info("Created table '%s'.", table_id)

    def _stage_and_merge(
        self,
        target_table_id: str,
        schema: list[bigquery.SchemaField],
        rows: list[dict],
        key_columns: list[str],
        update_columns: list[str] | None = None,
    ) -> None:
        if not rows:
            return

        staging_table_id = (
            f"{target_table_id}__staging_{int(datetime.now(timezone.utc).timestamp() * 1000)}"
        )

        staging_table = bigquery.Table(staging_table_id, schema=schema)
        staging_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
        self._client.create_table(staging_table, exists_ok=True)

        try:
            load_job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            load_job = self._client.load_table_from_json(
                rows, staging_table_id, job_config=load_job_config
            )
            load_job.result()

            if not self._dml_merge_enabled:
                self._append_from_staging(staging_table_id, target_table_id)
                return

            column_names = [field.name for field in schema]
            on_clause = " AND ".join([f"T.{k} = S.{k}" for k in key_columns])
            insert_columns = ", ".join(column_names)
            insert_values = ", ".join([f"S.{c}" for c in column_names])

            merge_parts = [
                f"MERGE `{target_table_id}` T",
                f"USING `{staging_table_id}` S",
                f"ON {on_clause}",
            ]

            if update_columns:
                update_clause = ", ".join([f"{c} = S.{c}" for c in update_columns])
                merge_parts.append(f"WHEN MATCHED THEN UPDATE SET {update_clause}")

            merge_parts.append(
                f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            )

            merge_sql = "\n".join(merge_parts)
            try:
                self._client.query(merge_sql).result()
            except Exception as err:
                if self._is_dml_billing_error(err):
                    self._dml_merge_enabled = False
                    log.warning(
                        "DML MERGE blocked by billing restrictions. "
                        "Falling back to append-only load for this and future batches."
                    )
                    self._append_from_staging(staging_table_id, target_table_id)
                else:
                    raise

        finally:
            self._client.delete_table(staging_table_id, not_found_ok=True)

    def _append_from_staging(self, staging_table_id: str, target_table_id: str) -> None:
        copy_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        copy_job = self._client.copy_table(
            sources=staging_table_id,
            destination=target_table_id,
            job_config=copy_config,
        )
        copy_job.result()

    @staticmethod
    def _is_dml_billing_error(err: Exception) -> bool:
        message = str(err).lower()
        return (
            "billingnotenabled" in message
            or "billing has not been enabled" in message
            or "dml queries are not allowed in the free tier" in message
        )

    def load(self, rows: list[dict]) -> int:
        """
        Load hourly rows into star schema tables.

        Returns the number of fact rows merged.
        """
        if not rows:
            return 0

        dim_location_rows: dict[str, dict] = {}
        dim_time_rows: dict[int, dict] = {}
        fact_rows: dict[str, dict] = {}

        for row in rows:
            dim_location, dim_time, fact = to_star_rows(row)

            location_key = dim_location["location_key"]
            existing_location = dim_location_rows.get(location_key)
            if existing_location is None:
                dim_location_rows[location_key] = dim_location
            else:
                if existing_location.get("latitude") is None and dim_location.get("latitude") is not None:
                    existing_location["latitude"] = dim_location["latitude"]
                if existing_location.get("longitude") is None and dim_location.get("longitude") is not None:
                    existing_location["longitude"] = dim_location["longitude"]

            dim_time_rows[dim_time["time_key"]] = dim_time
            fact_rows[fact["fact_id"]] = fact

        self._stage_and_merge(
            target_table_id=self._dim_location_table_id,
            schema=DIM_LOCATION_SCHEMA,
            rows=list(dim_location_rows.values()),
            key_columns=["location_key"],
            update_columns=["city", "hotspot", "latitude", "longitude"],
        )

        self._stage_and_merge(
            target_table_id=self._dim_time_table_id,
            schema=DIM_TIME_SCHEMA,
            rows=list(dim_time_rows.values()),
            key_columns=["time_key"],
            update_columns=None,
        )

        self._stage_and_merge(
            target_table_id=self._fact_table_id,
            schema=FACT_AIR_QUALITY_HOURLY_SCHEMA,
            rows=list(fact_rows.values()),
            key_columns=["fact_id"],
            update_columns=[
                "location_key",
                "time_key",
                "timestamp",
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
                "ingested_at",
            ],
        )

        merged_count = len(fact_rows)
        log.info(
            "Merged %d fact rows into %s (dim_location=%d, dim_time=%d)",
            merged_count,
            self._fact_table_id,
            len(dim_location_rows),
            len(dim_time_rows),
        )
        return merged_count
