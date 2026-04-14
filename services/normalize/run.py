"""Core normalization orchestration.

For each un-normalized table:
1. Classify all headers
2. Resolve mappings
3. Apply department-specific transform strategy
4. Generate fact_observation rows
5. Insert into curated tables
"""

import logging

from shared.clients.bigquery import BigQueryClient

from .classifiers.header_classifier import HeaderClassifier
from .config import NormalizeConfig
from .mappers.mapping_resolver import MappingResolver
from .transforms.wide_to_long import get_strategy

logger = logging.getLogger(__name__)


def normalize_batch(
    bq_client: BigQueryClient,
    config: NormalizeConfig,
    project_id: str,
    stg_dataset: str,
    cur_dataset: str,
    raw_dataset: str,
    department_id: str | None = None,
) -> dict:
    """Run normalization for un-processed tables."""
    classifier = HeaderClassifier()
    resolver = MappingResolver()

    # Find tables that have staging data but no fact observations yet
    where = ""
    if department_id:
        where = f"AND et.department_id = '{department_id}'"

    sql = f"""
        SELECT DISTINCT
            et.table_id,
            et.document_id,
            d.department_id,
            d.title
        FROM `{project_id}.{raw_dataset}.extracted_tables` et
        JOIN `{project_id}.{raw_dataset}.documents` d
            ON et.document_id = d.document_id
        LEFT JOIN `{project_id}.{cur_dataset}.fact_observation` fo
            ON et.table_id = fo.table_id
        WHERE fo.table_id IS NULL
        {where}
        LIMIT 1000
    """

    tables = bq_client.query(sql)
    logger.info("Found %d tables to normalize", len(tables))

    stats = {
        "tables_processed": 0,
        "observations_created": 0,
        "headers_classified": 0,
        "errors": 0,
    }

    for table_info in tables:
        try:
            table_stats = _normalize_table(
                bq_client=bq_client,
                classifier=classifier,
                resolver=resolver,
                config=config,
                project_id=project_id,
                stg_dataset=stg_dataset,
                cur_dataset=cur_dataset,
                table_id=table_info["table_id"],
                document_id=table_info["document_id"],
                department_id=table_info["department_id"],
                title=table_info.get("title", ""),
            )
            stats["tables_processed"] += 1
            stats["observations_created"] += table_stats.get("observations", 0)
            stats["headers_classified"] += table_stats.get("headers", 0)
        except Exception as e:
            logger.error("Error normalizing table %s: %s", table_info["table_id"], e)
            stats["errors"] += 1

    logger.info("Normalization batch complete: %s", stats)
    return stats


def _normalize_table(
    bq_client: BigQueryClient,
    classifier: HeaderClassifier,
    resolver: MappingResolver,
    config: NormalizeConfig,
    project_id: str,
    stg_dataset: str,
    cur_dataset: str,
    table_id: str,
    document_id: str,
    department_id: str,
    title: str,
) -> dict:
    """Normalize a single table."""
    # Fetch headers for this table
    headers_sql = f"""
        SELECT header_raw, header_normalized
        FROM `{project_id}.{stg_dataset}.headers`
        WHERE table_id = '{table_id}'
        ORDER BY header_id
    """
    header_rows = bq_client.query(headers_sql)
    headers = [h["header_raw"] for h in header_rows]

    if not headers:
        return {"headers": 0, "observations": 0}

    # Fetch row values for this table
    values_sql = f"""
        SELECT source_row_number, source_column_number, value_raw
        FROM `{project_id}.{stg_dataset}.row_values_long`
        WHERE table_id = '{table_id}'
        ORDER BY source_row_number, source_column_number
    """
    value_rows = bq_client.query(values_sql)

    # Reconstruct the table grid
    if not value_rows:
        return {"headers": len(headers), "observations": 0}

    max_row = max(v["source_row_number"] for v in value_rows)
    max_col = max(v["source_column_number"] for v in value_rows)

    grid: list[list[str | None]] = [[None] * (max_col + 1) for _ in range(max_row + 1)]
    for v in value_rows:
        grid[v["source_row_number"]][v["source_column_number"]] = v["value_raw"]

    # Apply department-specific transform
    strategy = get_strategy(department_id, classifier, resolver)
    observations = strategy.transform(
        headers=headers,
        rows=grid,
        department_id=department_id,
        document_id=document_id,
        table_id=table_id,
    )

    if len(observations) > config.max_observations_per_table:
        logger.warning(
            "Table %s produced %d observations, truncating to %d",
            table_id, len(observations), config.max_observations_per_table,
        )
        observations = observations[:config.max_observations_per_table]

    # Insert observations into curated fact table
    if observations:
        chunk_size = 500
        for i in range(0, len(observations), chunk_size):
            chunk = observations[i:i + chunk_size]
            bq_client.insert_rows(cur_dataset, "fact_observation", [o.to_bq_row() for o in chunk])

    return {"headers": len(headers), "observations": len(observations)}
