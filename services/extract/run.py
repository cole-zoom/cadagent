import hashlib
import logging
import math
from datetime import datetime, timezone

from shared.clients.bigquery import BigQueryClient
from shared.clients.gcs import GcsClient
from shared.models.table import ExtractedTable, HeaderRecord, RowValueLong
from shared.utils.text_normalization import detect_language, normalize_header

from .config import ExtractConfig
from .parsers.csv import parse_csv
from .parsers.html import parse_html
from .parsers.xlsx import parse_xlsx
from .parsers.xml import parse_xml

logger = logging.getLogger(__name__)

PARSER_MAP = {
    "csv": parse_csv,
    "html": parse_html,
    "xml": parse_xml,
}

# Magic byte prefixes for binary formats that should never reach a text parser.
# XLSX is technically a ZIP but is handled by its own binary parser, so excluded here.
BINARY_MAGIC_BYTES = (
    b"PK",              # ZIP / RAR-ish (XLSX handled separately via file_format check)
    b"\x1f\x8b",        # gzip
    b"%PDF",            # PDF
    b"\xd0\xcf\x11\xe0",  # OLE2 (old .xls, .doc)
    b"Rar!",            # RAR
    b"7z\xbc\xaf",      # 7z
)


def _is_binary_content(data: bytes, file_format: str) -> bool:
    """Detect binary content that shouldn't be parsed as text.

    XLSX/XLS are handled by their own binary parsers, so skip the check for them.
    """
    if file_format.lower() in ("xlsx", "xls"):
        return False
    if not data:
        return False
    for magic in BINARY_MAGIC_BYTES:
        if data.startswith(magic):
            return True
    # High ratio of control characters (excluding tab/newline/CR) = probably binary
    sample = data[:512]
    non_printable = sum(1 for b in sample if b < 0x20 and b not in (0x09, 0x0A, 0x0D))
    return non_printable / max(len(sample), 1) > 0.10


def extract_document(
    document_id: str,
    department_id: str,
    file_format: str,
    gcs_uri: str,
    title: str,
    gcs_client: GcsClient,
    bq_client: BigQueryClient,
    config: ExtractConfig,
    raw_bucket: str,
    processed_bucket: str,
    raw_dataset: str,
    stg_dataset: str,
) -> dict:
    """Extract tables from a single document. Returns stats dict."""
    logger.info("Extracting document=%s format=%s", document_id, file_format)

    try:
        data = gcs_client.download_file(gcs_uri)
    except Exception as e:
        logger.error("Failed to download %s: %s", gcs_uri, e)
        return {"tables": 0, "headers": 0, "rows": 0, "error": str(e)}

    if _is_binary_content(data, file_format):
        logger.warning(
            "Skipping document=%s format=%s — binary content detected (first bytes=%r)",
            document_id, file_format, data[:8],
        )
        return {"tables": 0, "headers": 0, "rows": 0, "skipped_reason": "binary_content"}

    tables = _route_to_parser(data, document_id, file_format, title)

    if len(tables) > config.max_tables_per_document:
        logger.warning(
            "Document %s produced %d tables, truncating to %d",
            document_id, len(tables), config.max_tables_per_document,
        )
        tables = tables[:config.max_tables_per_document]

    total_headers = 0
    total_rows = 0

    for table in tables:
        if len(table.headers) > config.max_columns:
            logger.warning("Skipping table %s with %d columns", table.table_id, len(table.headers))
            continue

        if len(table.rows) < config.min_rows_for_table:
            continue

        # Write extracted table metadata to BQ
        bq_client.insert_rows(raw_dataset, "extracted_tables", [table.to_bq_row()])

        # Process headers
        header_records = _build_header_records(table, department_id, document_id)
        if header_records:
            bq_client.insert_rows(stg_dataset, "headers", [h.to_bq_row() for h in header_records])
            total_headers += len(header_records)

        # Build row_values_long — streaming for small, load job for large
        row_values = _build_row_values(table, department_id, document_id, header_records)
        if row_values:
            rows_dicts = [rv.to_bq_row() for rv in row_values]
            if len(rows_dicts) > 2000:
                _load_rows(bq_client, stg_dataset, "row_values_long", rows_dicts)
            else:
                for i in range(0, len(rows_dicts), 500):
                    bq_client.insert_rows(stg_dataset, "row_values_long", rows_dicts[i:i + 500])
            total_rows += len(row_values)

    stats = {"tables": len(tables), "headers": total_headers, "rows": total_rows}
    logger.info("Extraction complete for %s: %s", document_id, stats)
    return stats


def _load_rows(bq_client: BigQueryClient, dataset: str, table: str, rows: list[dict]) -> None:
    """Bulk load rows via a load job instead of streaming inserts."""
    import io
    import json
    from google.cloud import bigquery

    ndjson = "\n".join(json.dumps(row) for row in rows)
    table_ref = f"{bq_client.project_id}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
    job = bq_client.client.load_table_from_file(
        io.BytesIO(ndjson.encode()), table_ref, job_config=job_config
    )
    job.result()
    logger.info("Loaded %d rows into %s via load job", len(rows), table_ref)


def _route_to_parser(
    data: bytes, document_id: str, file_format: str, title: str
) -> list[ExtractedTable]:
    fmt = file_format.lower()

    if fmt in ("xlsx", "xls"):
        return parse_xlsx(data, document_id, file_format=fmt)

    parser_func = PARSER_MAP.get(fmt)
    if parser_func:
        return parser_func(data, document_id, resource_name=title)

    logger.warning("No parser for format '%s' (document=%s)", file_format, document_id)
    return []


def _build_header_records(
    table: ExtractedTable, department_id: str, document_id: str
) -> list[HeaderRecord]:
    records = []
    for header_raw in table.headers:
        normalized = normalize_header(header_raw)
        header_id = hashlib.sha256(
            f"{table.table_id}|{header_raw}".encode()
        ).hexdigest()[:32]

        records.append(HeaderRecord(
            header_id=header_id,
            department_id=department_id,
            document_id=document_id,
            table_id=table.table_id,
            header_raw=header_raw,
            header_normalized=normalized,
            header_language=detect_language(header_raw),
        ))
    return records


def _build_row_values(
    table: ExtractedTable,
    department_id: str,
    document_id: str,
    header_records: list[HeaderRecord],
) -> list[RowValueLong]:
    values = []
    now = datetime.now(timezone.utc).isoformat()

    for row_idx, row in enumerate(table.rows):
        # First column is often the row label
        row_label = row[0] if row and row[0] else None

        for col_idx, cell_value in enumerate(row):
            if col_idx >= len(header_records):
                break

            header = header_records[col_idx]
            val_str = str(cell_value).strip() if cell_value else None

            staging_id = hashlib.sha256(
                f"{table.table_id}|{row_idx}|{col_idx}".encode()
            ).hexdigest()[:32]

            numeric_guess = _try_parse_numeric(val_str)

            values.append(RowValueLong(
                staging_value_id=staging_id,
                department_id=department_id,
                document_id=document_id,
                table_id=table.table_id,
                source_row_number=row_idx,
                source_column_number=col_idx,
                row_label_raw=row_label,
                header_id=header.header_id,
                header_raw=header.header_raw,
                value_raw=val_str,
                value_numeric_guess=numeric_guess,
                created_at=now,
            ))

    return values


def _try_parse_numeric(s: str | None) -> float | None:
    if not s:
        return None
    # Remove commas, spaces, dollar signs
    cleaned = s.replace(",", "").replace(" ", "").replace("$", "").replace("%", "").strip()
    if cleaned in ("", "-", "...", "x", "n/a", "na", "F", "E"):
        return None
    try:
        val = float(cleaned)
        if not math.isfinite(val):
            return None
        return val
    except (ValueError, TypeError):
        return None


def extract_batch(
    bq_client: BigQueryClient,
    gcs_client: GcsClient,
    config: ExtractConfig,
    raw_bucket: str,
    processed_bucket: str,
    raw_dataset: str,
    stg_dataset: str,
    project_id: str,
    department_id: str | None = None,
) -> dict:
    """Process all documents that have not yet been extracted."""
    where_clause = "WHERE d.ingestion_status = 'success'"
    if department_id:
        where_clause += f" AND d.department_id = '{department_id}'"

    sql = f"""
        SELECT d.document_id, d.department_id, d.file_format, d.gcs_uri, d.title
        FROM `{project_id}.{raw_dataset}.documents` d
        LEFT JOIN `{project_id}.{raw_dataset}.extracted_tables` et
            ON d.document_id = et.document_id
        {where_clause}
            AND et.document_id IS NULL
    """

    documents = bq_client.query(sql)
    logger.info("Found %d documents to extract", len(documents))

    total_stats = {"tables": 0, "headers": 0, "rows": 0, "documents": 0, "errors": 0}

    for doc in documents:
        stats = extract_document(
            document_id=doc["document_id"],
            department_id=doc["department_id"],
            file_format=doc["file_format"],
            gcs_uri=doc["gcs_uri"],
            title=doc.get("title", ""),
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket=raw_bucket,
            processed_bucket=processed_bucket,
            raw_dataset=raw_dataset,
            stg_dataset=stg_dataset,
        )
        total_stats["documents"] += 1
        total_stats["tables"] += stats["tables"]
        total_stats["headers"] += stats["headers"]
        total_stats["rows"] += stats["rows"]
        if "error" in stats:
            total_stats["errors"] += 1

    logger.info("Batch extraction complete: %s", total_stats)
    return total_stats
