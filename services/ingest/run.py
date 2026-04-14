import logging
import re
from datetime import datetime, timezone

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.clients.goc_api import GocApiClient
from shared.models.document import DocumentRecord
from shared.utils.hashing import compute_checksum, generate_document_id

from .config import IngestConfig

logger = logging.getLogger(__name__)

DOCUMENT_TYPE_PATTERNS = [
    (r"budget", "budget"),
    (r"estimate", "estimates"),
    (r"fiscal monitor", "fiscal_monitor"),
    (r"economic (and fiscal )?(update|statement)", "economic_update"),
    (r"survey.*forecast", "economic_survey"),
    (r"transfer", "transfer_tables"),
    (r"proactive disclosure", "proactive_disclosure"),
    (r"employee survey|pses", "employee_survey"),
    (r"infobase|covid", "expenditure_tracking"),
    (r"aid|iati", "international_aid"),
]


def infer_document_type(title: str) -> str:
    """Infer document type from dataset title."""
    title_lower = title.lower()
    for pattern, doc_type in DOCUMENT_TYPE_PATTERNS:
        if re.search(pattern, title_lower):
            return doc_type
    return "other"


def extract_year_from_title(title: str) -> str | None:
    """Try to extract a year or fiscal year from a dataset title."""
    match = re.search(r"(20\d{2})", title)
    return match.group(1) if match else None


def ingest_department(
    department_code: str,
    mode: str,
    api_client: GocApiClient,
    gcs_client: GcsClient,
    bq_client: BigQueryClient,
    config: IngestConfig,
    raw_bucket: str,
    raw_dataset: str,
) -> dict:
    """Ingest all datasets for a department.

    Returns a summary dict with counts of success/skipped/failed.
    """
    logger.info("Starting ingestion for department=%s mode=%s", department_code, mode)

    existing_urls: set[str] = set()
    if mode == "incremental":
        try:
            existing_urls = bq_client.get_existing_source_urls(raw_dataset, department_code)
            logger.info("Found %d existing URLs for %s", len(existing_urls), department_code)
        except Exception:
            logger.warning("Could not fetch existing URLs; treating as full ingestion")

    datasets = api_client.search_all_datasets(department_code)
    logger.info("Found %d datasets for %s", len(datasets), department_code)

    stats = {"success": 0, "skipped": 0, "failed": 0, "duplicate": 0}
    resources_processed = 0

    for dataset in datasets:
        if config.max_resources and resources_processed >= config.max_resources:
            logger.info("Hit max_resources limit (%d), stopping", config.max_resources)
            break

        dataset_title = GocApiClient.extract_title(dataset)
        document_type = infer_document_type(dataset_title)
        resources = api_client.list_resources(dataset)

        for resource in resources:
            if config.max_resources and resources_processed >= config.max_resources:
                break

            resource_url = resource.get("url", "")
            if not resource_url:
                continue

            if resource_url.startswith("/"):
                resource_url = f"https://open.canada.ca{resource_url}"

            if mode == "incremental" and resource_url in existing_urls:
                stats["duplicate"] += 1
                continue

            file_format = GocApiClient.extract_format(resource)
            language = GocApiClient.extract_language(resource)
            resource_name = resource.get("name", "") or resource.get("id", "unknown")

            try:
                data = api_client.download_resource(resource_url)
            except Exception as e:
                logger.warning("Failed to download %s: %s", resource_url, e)
                stats["failed"] += 1
                continue

            if len(data) > config.max_file_size_mb * 1024 * 1024:
                logger.warning("Skipping oversized file (%d bytes): %s", len(data), resource_url)
                stats["skipped"] += 1
                continue

            checksum = compute_checksum(data)
            doc_id = generate_document_id(resource_url, checksum)
            year = extract_year_from_title(dataset_title) or "unknown"
            filename = f"{resource_name}.{file_format}" if file_format else resource_name

            try:
                gcs_uri = gcs_client.upload_raw_file(
                    bucket_name=raw_bucket,
                    department=department_code,
                    year=year,
                    document_id=doc_id,
                    filename=filename,
                    data=data,
                )
            except Exception as e:
                logger.error("Failed to upload to GCS: %s", e)
                stats["failed"] += 1
                continue

            published_date = dataset.get("date_published")
            if published_date:
                published_date = published_date[:10]

            record = DocumentRecord(
                document_id=doc_id,
                department_id=department_code,
                department_code=department_code,
                gcs_uri=gcs_uri,
                source_url=resource_url,
                title=dataset_title,
                document_type=document_type,
                file_format=file_format,
                language=language,
                published_date=published_date,
                checksum=checksum,
                ingested_at=datetime.now(timezone.utc).isoformat(),
                ingestion_status="success",
            )

            try:
                errors = bq_client.insert_rows(raw_dataset, "documents", [record.to_bq_row()])
                if errors:
                    logger.error("BQ insert error for %s: %s", doc_id, errors)
                    stats["failed"] += 1
                else:
                    stats["success"] += 1
            except Exception as e:
                logger.error("BQ insert exception for %s: %s", doc_id, e)
                stats["failed"] += 1

            resources_processed += 1

    logger.info("Ingestion complete for %s: %s", department_code, stats)
    return stats
