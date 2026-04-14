import logging
from pathlib import Path

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def insert_rows(self, dataset: str, table: str, rows: list[dict]) -> list[dict]:
        """Streaming insert rows into a table. Returns errors if any."""
        table_ref = f"{self.project_id}.{dataset}.{table}"
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error("BigQuery insert errors for %s: %s", table_ref, errors)
        else:
            logger.info("Inserted %d rows into %s", len(rows), table_ref)
        return errors

    def query(self, sql: str, params: dict | None = None) -> list[dict]:
        """Run a parameterized query and return results as dicts."""
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
            ]

        job = self.client.query(sql, job_config=job_config)
        return [dict(row) for row in job.result()]

    def load_from_json(
        self, dataset: str, table: str, rows: list[dict], schema: list[bigquery.SchemaField]
    ) -> None:
        """Load rows via a load job (better for bulk inserts)."""
        table_ref = f"{self.project_id}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        import json
        import io

        ndjson = "\n".join(json.dumps(row) for row in rows)
        job = self.client.load_table_from_file(
            io.BytesIO(ndjson.encode()), table_ref, job_config=job_config
        )
        job.result()
        logger.info("Loaded %d rows into %s", len(rows), table_ref)

    def table_exists(self, dataset: str, table: str) -> bool:
        """Check if a table exists."""
        table_ref = f"{self.project_id}.{dataset}.{table}"
        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False

    def execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE TABLE, etc.)."""
        job = self.client.query(sql)
        job.result()
        logger.info("Executed DDL: %s...", sql[:80])

    def execute_ddl_file(self, path: Path, replacements: dict[str, str] | None = None) -> None:
        """Read and execute a SQL DDL file, optionally replacing placeholders."""
        sql = path.read_text()
        if replacements:
            for placeholder, value in replacements.items():
                sql = sql.replace(placeholder, value)
        self.execute_ddl(sql)

    def get_existing_source_urls(self, dataset: str, department_id: str) -> set[str]:
        """Get all source_url values for a department from raw.documents."""
        sql = f"""
            SELECT source_url
            FROM `{self.project_id}.{dataset}.documents`
            WHERE department_id = @department_id
        """
        rows = self.query(sql, params={"department_id": department_id})
        return {row["source_url"] for row in rows}
