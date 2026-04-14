CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_time` (
  time_id               STRING    NOT NULL,
  time_type             STRING    NOT NULL,
  label                 STRING    NOT NULL,
  start_date            DATE,
  end_date              DATE,
  fiscal_year_start_month INT64   DEFAULT 4,
  is_projection         BOOL      DEFAULT FALSE,
  created_at            TIMESTAMP
)
CLUSTER BY time_type
OPTIONS (
  description = 'Dimension table for time periods including fiscal years, quarters, and months. Supports the Canadian federal fiscal year starting in April and distinguishes actuals from projections.'
);
