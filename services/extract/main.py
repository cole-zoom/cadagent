import logging
import os
import sys

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

from .config import ExtractConfig
from .run import extract_batch, extract_document

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging(settings.log_level, service="extract")
    logger.info("Starting extract service")

    config = ExtractConfig()
    gcs_client = GcsClient(project_id=settings.gcp_project_id)
    bq_client = BigQueryClient(project_id=settings.gcp_project_id)

    # Single document mode
    document_id = os.getenv("DOCUMENT_ID")
    if document_id:
        doc_rows = bq_client.query(
            f"SELECT * FROM `{settings.gcp_project_id}.{settings.bq_raw_dataset}.documents` "
            f"WHERE document_id = @doc_id",
            params={"doc_id": document_id},
        )
        if not doc_rows:
            logger.error("Document %s not found", document_id)
            sys.exit(1)

        doc = doc_rows[0]
        extract_document(
            document_id=doc["document_id"],
            department_id=doc["department_id"],
            file_format=doc["file_format"],
            gcs_uri=doc["gcs_uri"],
            title=doc.get("title", ""),
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket=settings.gcs_raw_bucket,
            processed_bucket=settings.gcs_processed_bucket,
            raw_dataset=settings.bq_raw_dataset,
            stg_dataset=settings.bq_stg_dataset,
        )
        return

    # Batch mode
    department = os.getenv("DEPARTMENT")
    stats = extract_batch(
        bq_client=bq_client,
        gcs_client=gcs_client,
        config=config,
        raw_bucket=settings.gcs_raw_bucket,
        processed_bucket=settings.gcs_processed_bucket,
        raw_dataset=settings.bq_raw_dataset,
        stg_dataset=settings.bq_stg_dataset,
        project_id=settings.gcp_project_id,
        department_id=department,
    )

    if stats["errors"] > 0 and stats["documents"] == stats["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
