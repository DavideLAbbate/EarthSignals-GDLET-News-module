"""
BigQuery client wrapper.

google-cloud-bigquery is a synchronous library. To avoid blocking the
async event loop, ALL BigQuery calls are wrapped with run_in_executor
using a bounded ThreadPoolExecutor.

The client is a singleton stored in app.state; the executor is created
once at startup and shut down cleanly on application shutdown.
"""

from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

from app.core.config import get_settings
from app.core.exceptions import BigQueryError
from app.core.logging import get_logger

logger = get_logger(__name__)


class BigQueryClientWrapper:
    """
    Wraps google.cloud.bigquery.Client to expose an async interface.

    All blocking BQ calls are offloaded to a dedicated thread pool,
    keeping the uvicorn event loop free for request handling.
    """

    def __init__(self, client: bigquery.Client, executor: ThreadPoolExecutor) -> None:
        self._client = client
        self._executor = executor

    async def run_query(
        self,
        query: str,
        query_params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter],
    ) -> list[dict[str, Any]]:
        """
        Execute a BigQuery query asynchronously.

        Runs the synchronous BQ client in the thread pool executor.
        Returns a list of row dicts. Raises BigQueryError on failure.
        """
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        def _execute() -> list[dict[str, Any]]:
            try:
                query_job = self._client.query(query, job_config=job_config)
                results = query_job.result()  # blocks until complete
                bytes_processed = query_job.total_bytes_processed or 0
                logger.info(
                    "bigquery_query_complete",
                    bytes_processed=bytes_processed,
                    rows_returned=results.total_rows,
                )
                return [dict(row) for row in results]
            except Exception as exc:
                raise BigQueryError(
                    f"BigQuery query failed: {exc}",
                    detail=str(exc),
                ) from exc

        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(self._executor, _execute)
        except BigQueryError:
            raise
        except Exception as exc:
            raise BigQueryError(
                f"Unexpected error in BigQuery executor: {exc}",
                detail=str(exc),
            ) from exc

    async def run_query_single_value(
        self,
        query: str,
        query_params: list | None = None,
    ) -> Any:
        """Execute a query and return the first column of the first row."""
        rows = await self.run_query(query, query_params or [])
        if not rows:
            return None
        first_row = rows[0]
        return next(iter(first_row.values()), None)

    def shutdown(self) -> None:
        """Gracefully shut down the thread pool executor."""
        self._executor.shutdown(wait=True)
        logger.info("bigquery_executor_shutdown")


def create_bigquery_client() -> BigQueryClientWrapper:
    """
    Instantiate the BigQuery client singleton.

    Reads credentials from GOOGLE_APPLICATION_CREDENTIALS env var
    (set by Docker Compose volume mount of the GCP service account JSON).
    """
    settings = get_settings()

    try:
        credentials = _load_bigquery_credentials(settings.gcp_service_account_json)
        client = bigquery.Client(project=settings.gcp_project_id, credentials=credentials)
    except Exception as exc:
        raise BigQueryError(
            f"Failed to create BigQuery client: {exc}. "
            "Ensure GOOGLE_APPLICATION_CREDENTIALS points to a valid service account key, "
            "or set GCP_SERVICE_ACCOUNT_JSON to a valid service account JSON payload.",
            detail=str(exc),
        ) from exc

    executor = ThreadPoolExecutor(
        max_workers=settings.bq_executor_max_workers,
        thread_name_prefix="bq-worker",
    )
    logger.info(
        "bigquery_client_created",
        project=settings.gcp_project_id,
        executor_workers=settings.bq_executor_max_workers,
    )
    return BigQueryClientWrapper(client, executor)


def _load_bigquery_credentials(service_account_json: str | None):
    """Load credentials from inline service account JSON when available."""
    if not service_account_json:
        return None

    try:
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError as exc:
        raise BigQueryError(
            "Invalid GCP service account JSON provided in GCP_SERVICE_ACCOUNT_JSON.",
            detail=str(exc),
        ) from exc

    try:
        return service_account.Credentials.from_service_account_info(service_account_info)
    except Exception as exc:
        raise BigQueryError(
            "Failed to load credentials from GCP service account JSON.",
            detail=str(exc),
        ) from exc
