"""Tool definitions and dispatchers for the agent loop.

Each tool has a JSON-schema that Claude uses to call it, plus a dispatcher
that runs the actual BigQuery / validator logic and returns a string result
the model can reason about.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from shared.clients.bigquery import BigQueryClient

from .sql_validator import SQLValidationError, validate_sql

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Tool schemas — passed to Claude as the `tools=[...]` argument
# ──────────────────────────────────────────────────────────────────────────

TOOLS: list[dict[str, Any]] = [
    {
        "name": "query_data",
        "description": (
            "Execute a SELECT SQL query against the curated data warehouse "
            "(`cur.fact_observation` and related dim tables). Use this to "
            "answer questions about specific values, trends, or comparisons. "
            "Automatically limited to 100 rows. Use fully qualified table "
            "names with backticks like `cur.fact_observation`. Returns JSON "
            "rows plus a count."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SELECT query to run.",
                },
                "reason": {
                    "type": "string",
                    "description": "Brief one-line reason for this query.",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "list_metrics",
        "description": (
            "List available metric names in `cur.dim_metric`. Use this to "
            "discover what measures exist before writing a query, or when a "
            "filter returned 0 results and you need to find a close match. "
            "Supports substring search."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "search": {
                    "type": "string",
                    "description": "Substring to match (case-insensitive). Omit to list top metrics by observation count.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 25).",
                },
            },
        },
    },
    {
        "name": "list_time_periods",
        "description": (
            "List distinct time labels available in `cur.dim_time` ordered "
            "by how many observations reference them. Use this when the user "
            "asks about a time period you're unsure the data covers."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 40)."},
            },
        },
    },
    {
        "name": "list_geographies",
        "description": (
            "List named geographies in `cur.dim_geography`. There are only "
            "~22 — mostly Canadian provinces + Canada aggregate. Most "
            "fact_observation rows have NULL geography_id."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "describe_coverage",
        "description": (
            "Return a high-level summary of what data is available: "
            "observation counts by department, metric counts, time range. "
            "Use this for 'what do you have?' or 'what can I ask?' "
            "meta-questions."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
]


# ──────────────────────────────────────────────────────────────────────────
# Dispatcher
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class ToolResult:
    """Result of a single tool invocation."""
    content: str  # String representation fed back to Claude
    document_ids: set[str]  # Doc ids touched, aggregated for citation lookup
    is_error: bool = False


def dispatch_tool(
    name: str,
    tool_input: dict[str, Any],
    bq: BigQueryClient,
    project_id: str,
    cur_dataset: str,
) -> ToolResult:
    """Dispatch a tool call and return a string-serialized result."""
    try:
        if name == "query_data":
            return _query_data(tool_input, bq, project_id, cur_dataset)
        if name == "list_metrics":
            return _list_metrics(tool_input, bq, project_id, cur_dataset)
        if name == "list_time_periods":
            return _list_time_periods(tool_input, bq, project_id, cur_dataset)
        if name == "list_geographies":
            return _list_geographies(bq, project_id, cur_dataset)
        if name == "describe_coverage":
            return _describe_coverage(bq, project_id, cur_dataset)
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return ToolResult(
            content=f"Tool error: {type(e).__name__}: {e}",
            document_ids=set(),
            is_error=True,
        )

    return ToolResult(
        content=f"Unknown tool: {name}",
        document_ids=set(),
        is_error=True,
    )


# ──────────────────────────────────────────────────────────────────────────
# Individual tool implementations
# ──────────────────────────────────────────────────────────────────────────


def _query_data(
    tool_input: dict[str, Any],
    bq: BigQueryClient,
    project_id: str,
    cur_dataset: str,
) -> ToolResult:
    raw_sql = tool_input.get("sql", "").strip()
    if not raw_sql:
        return ToolResult("Error: sql is required", set(), is_error=True)

    try:
        sql = validate_sql(raw_sql, project_id)
    except SQLValidationError as e:
        return ToolResult(f"SQL validation failed: {e}", set(), is_error=True)

    sql = _qualify_tables(sql, project_id)

    try:
        rows = bq.query(sql)
    except Exception as e:
        return ToolResult(
            f"Query execution failed: {e}",
            set(),
            is_error=True,
        )

    doc_ids = {r["document_id"] for r in rows if r.get("document_id")}

    # Truncate the result payload so Claude doesn't get overwhelmed.
    preview = rows[:30]
    payload = {
        "rows_returned": len(rows),
        "rows_shown": len(preview),
        "rows": preview,
    }
    return ToolResult(
        content=json.dumps(payload, default=str),
        document_ids=doc_ids,
    )


def _list_metrics(
    tool_input: dict[str, Any],
    bq: BigQueryClient,
    project_id: str,
    cur_dataset: str,
) -> ToolResult:
    search = tool_input.get("search", "").strip()
    limit = int(tool_input.get("limit") or 25)
    limit = max(1, min(limit, 100))

    where_clauses = ["dm.canonical_name IS NOT NULL", "dm.canonical_name != ''"]
    if search:
        # Keep quoting safe via a parameter-style lookup
        esc = search.replace("'", "''")
        where_clauses.append(f"LOWER(dm.canonical_name) LIKE LOWER('%{esc}%')")

    sql = f"""
        SELECT dm.canonical_name, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.dim_metric` dm
        LEFT JOIN `{project_id}.{cur_dataset}.fact_observation` f
          USING (metric_id)
        WHERE {' AND '.join(where_clauses)}
        GROUP BY dm.canonical_name
        HAVING COUNT(f.observation_id) > 0
        ORDER BY obs DESC
        LIMIT {limit}
    """
    rows = bq.query(sql)

    if not rows:
        return ToolResult(
            json.dumps({"matches": [], "note": "No metrics matched."}),
            set(),
        )

    return ToolResult(
        content=json.dumps({"matches": rows}, default=str),
        document_ids=set(),
    )


def _list_time_periods(
    tool_input: dict[str, Any],
    bq: BigQueryClient,
    project_id: str,
    cur_dataset: str,
) -> ToolResult:
    limit = int(tool_input.get("limit") or 40)
    limit = max(1, min(limit, 100))
    sql = f"""
        SELECT dt.label, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.dim_time` dt
        LEFT JOIN `{project_id}.{cur_dataset}.fact_observation` f
          USING (time_id)
        GROUP BY dt.label
        HAVING COUNT(f.observation_id) > 0
        ORDER BY obs DESC
        LIMIT {limit}
    """
    rows = bq.query(sql)
    return ToolResult(json.dumps({"periods": rows}, default=str), set())


def _list_geographies(
    bq: BigQueryClient, project_id: str, cur_dataset: str
) -> ToolResult:
    sql = f"""
        SELECT dg.name_en, dg.geo_type, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.dim_geography` dg
        LEFT JOIN `{project_id}.{cur_dataset}.fact_observation` f
          USING (geography_id)
        GROUP BY dg.name_en, dg.geo_type
        ORDER BY obs DESC
    """
    rows = bq.query(sql)
    return ToolResult(
        json.dumps(
            {
                "geographies": rows,
                "note": "Most fact_observation rows have NULL geography_id and won't match any of these.",
            },
            default=str,
        ),
        set(),
    )


def _describe_coverage(
    bq: BigQueryClient, project_id: str, cur_dataset: str
) -> ToolResult:
    """One-shot full discovery: per-department counts, top metrics, time periods,
    named geographies. Designed so the agent rarely needs to call list_* tools
    separately."""
    coverage_sql = f"""
        SELECT
          department_id,
          COUNT(*) AS observations,
          COUNT(DISTINCT metric_id) AS distinct_metrics,
          COUNT(DISTINCT document_id) AS documents
        FROM `{project_id}.{cur_dataset}.fact_observation`
        GROUP BY department_id
        ORDER BY observations DESC
    """
    metrics_sql = f"""
        SELECT dm.canonical_name, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.fact_observation` f
        JOIN `{project_id}.{cur_dataset}.dim_metric` dm USING (metric_id)
        WHERE dm.canonical_name IS NOT NULL AND dm.canonical_name != ''
        GROUP BY dm.canonical_name
        ORDER BY obs DESC
        LIMIT 30
    """
    times_sql = f"""
        SELECT dt.label, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.fact_observation` f
        JOIN `{project_id}.{cur_dataset}.dim_time` dt USING (time_id)
        GROUP BY dt.label
        ORDER BY obs DESC
    """
    geos_sql = f"""
        SELECT dg.name_en, dg.geo_type, COUNT(f.observation_id) AS obs
        FROM `{project_id}.{cur_dataset}.fact_observation` f
        JOIN `{project_id}.{cur_dataset}.dim_geography` dg USING (geography_id)
        GROUP BY dg.name_en, dg.geo_type
        ORDER BY obs DESC
    """
    return ToolResult(
        json.dumps(
            {
                "coverage_by_department": bq.query(coverage_sql),
                "top_30_metrics": bq.query(metrics_sql),
                "time_periods": bq.query(times_sql),
                "named_geographies_with_data": bq.query(geos_sql),
                "notes": [
                    "department_id values: fin (Finance Canada), statcan (Statistics Canada), tbs-sct (Treasury Board Secretariat)",
                    "StatCan data is mostly 2011 / 2016 census population + density.",
                    "Finance data is mostly tax expenditures.",
                    "Most fact_observation rows have geography_id = NULL.",
                ],
            },
            default=str,
        ),
        set(),
    )


def _qualify_tables(sql: str, project_id: str) -> str:
    """Replace `cur.table` with `project.cur.table`."""
    for dataset in ("cur", "quality"):
        sql = sql.replace(f"`{dataset}.", f"`{project_id}.{dataset}.")
    return sql
