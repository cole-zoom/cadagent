CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.fact_observation` (
  observation_id       STRING    NOT NULL,
  department_id        STRING    NOT NULL,
  document_id          STRING    NOT NULL,
  table_id             STRING,
  metric_id            STRING    NOT NULL,
  time_id              STRING    NOT NULL,
  geography_id         STRING,
  scenario_id          STRING,
  value_numeric        FLOAT64,
  value_text           STRING,
  unit_raw             STRING,
  scale_factor         FLOAT64   DEFAULT 1,
  currency_code        STRING,
  source_row_number    INT64,
  source_column_number INT64,
  quality_score        FLOAT64,
  created_at           TIMESTAMP NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, DAY)
CLUSTER BY department_id, metric_id, time_id
OPTIONS (
  description = 'Central fact table storing individual numeric and textual observations extracted from government documents. Each row links a measured value to its department, document, metric, time period, geography, and scenario. Partitioned by created_at and clustered for efficient analytical queries.'
);
