"""CSV parser for GoC data files.

Handles:
- Standard CSV files
- Encoding detection (UTF-8, Latin-1)
- Empty first columns (row labels)
- Files with metadata rows before the actual table
"""

import csv
import hashlib
import io
import logging

from shared.models.table import ExtractedTable

logger = logging.getLogger(__name__)

PARSER_VERSION = "0.1.0"


def parse_csv(
    data: bytes,
    document_id: str,
    resource_name: str = "",
    table_index_offset: int = 0,
) -> list[ExtractedTable]:
    """Parse a CSV file into ExtractedTable objects."""
    text = _decode(data)
    if not text.strip():
        return []

    try:
        reader = csv.reader(io.StringIO(text, newline=""))
        all_rows = list(reader)
    except csv.Error:
        # Fallback: replace bare newlines in unquoted fields and retry
        try:
            cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
            reader = csv.reader(cleaned.splitlines())
            all_rows = list(reader)
        except csv.Error as e:
            logger.warning("Failed to parse CSV for %s: %s", document_id, e)
            return []

    if len(all_rows) < 2:
        return []

    headers = all_rows[0]
    data_rows = all_rows[1:]

    # Drop any column whose header is empty/whitespace (interior or trailing).
    # These carry no analytic meaning and produce garbage rows downstream.
    valid_col_indices = [i for i, h in enumerate(headers) if h and h.strip()]
    if not valid_col_indices:
        return []
    if len(valid_col_indices) < len(headers):
        headers = [headers[i] for i in valid_col_indices]
        data_rows = [
            [row[i] if i < len(row) else None for i in valid_col_indices]
            for row in data_rows
        ]

    # Filter out completely empty rows
    data_rows = [row for row in data_rows if any(cell.strip() for cell in row if cell)]

    if not data_rows:
        return []

    table_id = hashlib.sha256(f"{document_id}|csv|{table_index_offset}".encode()).hexdigest()[:32]

    # Pad rows to match header length
    header_len = len(headers)
    padded_rows = []
    for row in data_rows:
        if len(row) < header_len:
            row = row + [None] * (header_len - len(row))
        elif len(row) > header_len:
            row = row[:header_len]
        padded_rows.append(row)

    table = ExtractedTable(
        table_id=table_id,
        document_id=document_id,
        table_index=table_index_offset,
        extraction_method="csv_parser",
        parser_version=PARSER_VERSION,
        headers=headers,
        rows=padded_rows,
        table_title_raw=resource_name or None,
    )

    return [table]


def _decode(data: bytes) -> str:
    """Try UTF-8 first, then Latin-1."""
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except (UnicodeDecodeError, ValueError):
            continue
    return data.decode("utf-8", errors="replace")
