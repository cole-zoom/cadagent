"""Backfill (full re-ingestion) for a single department.

Usage:
    python scripts/backfill_department.py --department fin
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.clients.goc_api import GocApiClient
from shared.config.logging import configure_logging
from shared.config.settings import settings
from services.ingest.config import IngestConfig
from services.ingest.run import ingest_department


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill ingestion for a department")
    parser.add_argument("--department", required=True, help="Department code (fin, statcan, tbs-sct)")
    args = parser.parse_args()

    configure_logging(settings.log_level, service="backfill")

    config = IngestConfig()
    api_client = GocApiClient(base_url=settings.goc_api_base_url, rate_limit_delay=config.rate_limit_delay)
    gcs_client = GcsClient(project_id=settings.gcp_project_id)
    bq_client = BigQueryClient(project_id=settings.gcp_project_id)

    stats = ingest_department(
        department_code=args.department,
        mode="full",
        api_client=api_client,
        gcs_client=gcs_client,
        bq_client=bq_client,
        config=config,
        raw_bucket=settings.gcs_raw_bucket,
        raw_dataset=settings.bq_raw_dataset,
    )
    print(f"Backfill complete: {stats}")


if __name__ == "__main__":
    main()
