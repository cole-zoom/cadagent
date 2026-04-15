"""HTML table parser for GoC data files."""

import hashlib
import logging

from shared.models.table import ExtractedTable

logger = logging.getLogger(__name__)

PARSER_VERSION = "0.1.0"


def parse_html(
    data: bytes,
    document_id: str,
    resource_name: str = "",
) -> list[ExtractedTable]:
    """Extract tables from an HTML file using BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("beautifulsoup4 not installed")
        return []

    try:
        soup = BeautifulSoup(data, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(data, "html.parser")
        except Exception as e:
            logger.warning("Failed to parse HTML for %s: %s", document_id, e)
            return []

    html_tables = soup.find_all("table")
    tables = []

    for idx, html_table in enumerate(html_tables):
        rows_data = []
        for tr in html_table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [cell.get_text(strip=True) for cell in cells]
            rows_data.append(row)

        if len(rows_data) < 2:
            continue

        headers = rows_data[0]
        data_rows = rows_data[1:]

        # Filter empty rows
        data_rows = [r for r in data_rows if any(c.strip() for c in r if c)]

        if not data_rows:
            continue

        # Pad rows first so column indices line up
        max_cols = max(len(headers), max((len(r) for r in data_rows), default=0))
        headers = headers + [""] * (max_cols - len(headers))
        data_rows = [r + [None] * (max_cols - len(r)) for r in data_rows]

        # Drop columns with empty headers
        valid_col_indices = [i for i, h in enumerate(headers) if h and h.strip()]
        if not valid_col_indices:
            continue
        if len(valid_col_indices) < len(headers):
            headers = [headers[i] for i in valid_col_indices]
            data_rows = [
                [row[i] if i < len(row) else None for i in valid_col_indices]
                for row in data_rows
            ]

        table_id = hashlib.sha256(
            f"{document_id}|html|{idx}".encode()
        ).hexdigest()[:32]

        tables.append(ExtractedTable(
            table_id=table_id,
            document_id=document_id,
            table_index=idx,
            extraction_method="html_parser",
            parser_version=PARSER_VERSION,
            headers=headers,
            rows=data_rows,
            table_title_raw=resource_name or None,
        ))

    return tables
