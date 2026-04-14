import logging
import os
import sys

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.clients.goc_api import GocApiClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

from .config import IngestConfig
from .run import ingest_department

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging(settings.log_level, service="ingest")
    logger.info("Starting ingest service")

    config = IngestConfig(
        max_resources=int(os.getenv("MAX_RESOURCES", "0")),
    )
    mode = os.getenv("INGEST_MODE", "incremental")

    api_client = GocApiClient(
        base_url=settings.goc_api_base_url,
        rate_limit_delay=config.rate_limit_delay,
    )
    gcs_client = GcsClient(project_id=settings.gcp_project_id)
    bq_client = BigQueryClient(project_id=settings.gcp_project_id)

    departments = os.getenv("DEPARTMENTS", "").split(",") if os.getenv("DEPARTMENTS") else settings.departments_list
    departments = [d.strip() for d in departments if d.strip()]

    total_stats = {"success": 0, "skipped": 0, "failed": 0, "duplicate": 0}

    for dept in departments:
        stats = ingest_department(
            department_code=dept,
            mode=mode,
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket=settings.gcs_raw_bucket,
            raw_dataset=settings.bq_raw_dataset,
        )
        for k, v in stats.items():
            total_stats[k] += v

    logger.info("All departments complete: %s", total_stats)

    if total_stats["failed"] > 0 and total_stats["success"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
