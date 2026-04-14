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

    reader = csv.reader(io.StringIO(text))
    all_rows = list(reader)

    if len(all_rows) < 2:
        return []

    headers = all_rows[0]
    data_rows = all_rows[1:]

    # Clean empty trailing columns
    while headers and not headers[-1].strip():
        col_idx = len(headers) - 1
        headers = headers[:col_idx]
        data_rows = [row[:col_idx] for row in data_rows]

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
