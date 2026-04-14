"""Source citation lookup for agent answers.

Given document_ids from query results, looks up full provenance
including source URLs, table titles, and page/sheet info.
"""

import logging
from dataclasses import dataclass

from shared.clients.bigquery import BigQueryClient

logger = logging.getLogger(__name__)


@dataclass
class Citation:
    document_id: str
    title: str
    source_url: str
    department: str
    table_title: str | None = None
    sheet_name: str | None = None
    page_number: int | None = None


def lookup_citations(
    bq_client: BigQueryClient,
    project_id: str,
    cur_dataset: str,
    raw_dataset: str,
    document_ids: list[str],
) -> list[Citation]:
    """Look up full provenance for a set of document IDs."""
    if not document_ids:
        return []

    id_list = ", ".join(f"'{did}'" for did in set(document_ids))

    sql = f"""
        SELECT
            dd.document_id,
            dd.title,
            dd.source_url,
            dd.department_id,
            et.table_title_raw,
            et.sheet_name,
            et.page_number
        FROM `{project_id}.{cur_dataset}.dim_document` dd
        LEFT JOIN `{project_id}.{raw_dataset}.extracted_tables` et
            ON dd.document_id = et.document_id
        WHERE dd.document_id IN ({id_list})
    """

    try:
        rows = bq_client.query(sql)
    except Exception as e:
        logger.error("Citation lookup failed: %s", e)
        return []

    # Deduplicate by document_id (take first table info)
    seen: dict[str, Citation] = {}
    for row in rows:
        did = row["document_id"]
        if did not in seen:
            seen[did] = Citation(
                document_id=did,
                title=row.get("title", ""),
                source_url=row.get("source_url", ""),
                department=row.get("department_id", ""),
                table_title=row.get("table_title_raw"),
                sheet_name=row.get("sheet_name"),
                page_number=row.get("page_number"),
            )

    return list(seen.values())


def format_citations(citations: list[Citation]) -> str:
    """Format citations as a readable string for the LLM."""
    if not citations:
        return "No source documents found."

    lines = []
    for c in citations:
        parts = [f"- {c.title}"]
        if c.source_url:
            parts.append(f"  URL: {c.source_url}")
        if c.department:
            parts.append(f"  Department: {c.department}")
        if c.table_title:
            parts.append(f"  Table: {c.table_title}")
        lines.append("\n".join(parts))

    return "\n\n".join(lines)
