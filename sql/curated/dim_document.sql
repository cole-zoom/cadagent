CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_document` (
  document_id     STRING    NOT NULL,
  department_id   STRING    NOT NULL,
  title           STRING,
  document_type   STRING,
  language        STRING,
  published_date  DATE,
  fiscal_year_label STRING,
  source_url      STRING,
  gcs_uri         STRING,
  parser_version  STRING,
  created_at      TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(created_at, DAY)
CLUSTER BY department_id
OPTIONS (
  description = 'Dimension table for source documents ingested into the pipeline. Tracks provenance including GCS location, parser version, and publication metadata. Partitioned by created_at for efficient incremental loads.'
);
