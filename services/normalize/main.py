import logging
import os
import sys

from shared.clients.bigquery import BigQueryClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

from .config import NormalizeConfig
from .run import normalize_batch

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging(settings.log_level, service="normalize")
    logger.info("Starting normalize service")

    config = NormalizeConfig()
    bq_client = BigQueryClient(project_id=settings.gcp_project_id)

    department = os.getenv("DEPARTMENT")

    stats = normalize_batch(
        bq_client=bq_client,
        config=config,
        project_id=settings.gcp_project_id,
        stg_dataset=settings.bq_stg_dataset,
        cur_dataset=settings.bq_cur_dataset,
        raw_dataset=settings.bq_raw_dataset,
        department_id=department,
    )

    logger.info("Normalization complete: %s", stats)

    if stats["errors"] > 0 and stats["tables_processed"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
