"""Re-extract a single document by ID.

Usage:
    python scripts/reprocess_document.py --document-id abc123
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.config.logging import configure_logging
from shared.config.settings import settings
from services.extract.config import ExtractConfig
from services.extract.run import extract_document


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-extract a single document")
    parser.add_argument("--document-id", required=True, help="Document ID to reprocess")
    args = parser.parse_args()

    configure_logging(settings.log_level, service="reprocess")

    bq_client = BigQueryClient(project_id=settings.gcp_project_id)
    gcs_client = GcsClient(project_id=settings.gcp_project_id)

    doc_rows = bq_client.query(
        f"SELECT * FROM `{settings.gcp_project_id}.{settings.bq_raw_dataset}.documents` "
        f"WHERE document_id = @doc_id",
        params={"doc_id": args.document_id},
    )
    if not doc_rows:
        print(f"Document {args.document_id} not found")
        sys.exit(1)

    doc = doc_rows[0]
    stats = extract_document(
        document_id=doc["document_id"],
        department_id=doc["department_id"],
        file_format=doc["file_format"],
        gcs_uri=doc["gcs_uri"],
        title=doc.get("title", ""),
        gcs_client=gcs_client,
        bq_client=bq_client,
        config=ExtractConfig(),
        raw_bucket=settings.gcs_raw_bucket,
        processed_bucket=settings.gcs_processed_bucket,
        raw_dataset=settings.bq_raw_dataset,
        stg_dataset=settings.bq_stg_dataset,
    )
    print(f"Reprocess complete: {stats}")


if __name__ == "__main__":
    main()
