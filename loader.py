"""
BigQuery batch loader.

"""

import logging
from google.cloud import bigquery
from google.oauth2 import service_account

import config
from schema import TABLE_SCHEMA

log = logging.getLogger(__name__)


class BigQueryLoader:
    """Manages the BigQuery client, dataset/table creation, and batch loads."""

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            config.SERVICE_ACCOUNT_KEY,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        self._client = bigquery.Client(
            project=config.GCP_PROJECT_ID,
            credentials=credentials,
        )
        self._table_id = config.BQ_TABLE_ID
        self._ensure_dataset_and_table()

    # ── Setup ──────────────────────────────────────────────────────

    def _ensure_dataset_and_table(self):
        """Create the dataset and table if they don't already exist."""
        dataset_ref = bigquery.DatasetReference(
            config.GCP_PROJECT_ID, config.BQ_DATASET
        )

        # Dataset
        try:
            self._client.get_dataset(dataset_ref)
            log.info("Dataset '%s' already exists.", config.BQ_DATASET)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self._client.create_dataset(dataset, exists_ok=True)
            log.info("Created dataset '%s'.", config.BQ_DATASET)

        # Table
        table_ref = dataset_ref.table(config.BQ_TABLE)
        try:
            self._client.get_table(table_ref)
            log.info("Table '%s' already exists.", self._table_id)
        except Exception:
            table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
            self._client.create_table(table, exists_ok=True)
            log.info("Created table '%s'.", self._table_id)

    # ── Loading ────────────────────────────────────────────────────

    def load(self, rows: list[dict]) -> int:
        """
        Batch-load a list of row dicts into BigQuery.

        Returns the number of rows successfully loaded.
        Raises on unrecoverable errors so the caller can decide
        whether to retry or skip.
        """
        if not rows:
            return 0

        job_config = bigquery.LoadJobConfig(
            schema=TABLE_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = self._client.load_table_from_json(
            rows, self._table_id, job_config=job_config
        )
        job.result()  # block until done

        log.info(
            "Loaded %d rows → %s  (job %s)",
            job.output_rows, self._table_id, job.job_id,
        )
        return job.output_rows
