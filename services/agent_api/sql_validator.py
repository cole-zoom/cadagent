"""SQL validation layer for agent-generated queries.

Rejects mutations, enforces LIMIT, restricts to curated tables only.
"""

import re

FORBIDDEN_KEYWORDS = [
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bDROP\b",
    r"\bCREATE\b", r"\bALTER\b", r"\bTRUNCATE\b", r"\bMERGE\b",
    r"\bGRANT\b", r"\bREVOKE\b",
]

ALLOWED_DATASETS = {"cur", "quality"}

MAX_LIMIT = 1000


class SQLValidationError(Exception):
    pass


def validate_sql(sql: str, project_id: str) -> str:
    """Validate and sanitize agent-generated SQL. Returns cleaned SQL or raises."""
    sql_upper = sql.upper().strip()

    # Must be a SELECT
    if not sql_upper.startswith("SELECT"):
        raise SQLValidationError("Only SELECT queries are allowed")

    # Check for forbidden keywords
    for pattern in FORBIDDEN_KEYWORDS:
        if re.search(pattern, sql_upper):
            raise SQLValidationError(f"Forbidden SQL keyword detected: {pattern}")

    # Check that all table references are in allowed datasets
    table_refs = re.findall(r"`([^`]+)`", sql)
    for ref in table_refs:
        parts = ref.split(".")
        if len(parts) >= 2:
            dataset = parts[-2] if len(parts) == 3 else parts[0]
            if dataset not in ALLOWED_DATASETS:
                raise SQLValidationError(
                    f"Query references non-curated dataset '{dataset}'. "
                    f"Only {ALLOWED_DATASETS} are allowed."
                )

    # Ensure LIMIT exists, add if missing
    if "LIMIT" not in sql_upper:
        sql = sql.rstrip().rstrip(";") + f"\nLIMIT {MAX_LIMIT}"
    else:
        limit_match = re.search(r"LIMIT\s+(\d+)", sql_upper)
        if limit_match:
            limit_val = int(limit_match.group(1))
            if limit_val > MAX_LIMIT:
                sql = re.sub(
                    r"LIMIT\s+\d+", f"LIMIT {MAX_LIMIT}", sql, flags=re.IGNORECASE
                )

    return sql
