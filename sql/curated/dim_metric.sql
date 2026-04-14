CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_metric` (
  metric_id         STRING    NOT NULL,
  canonical_name    STRING    NOT NULL,
  canonical_name_fr STRING,
  metric_family     STRING,
  default_unit_id   STRING,
  description       STRING,
  is_additive       BOOL      DEFAULT FALSE,
  created_at        TIMESTAMP
)
CLUSTER BY metric_family
OPTIONS (
  description = 'Dimension table for canonical metrics extracted from government documents. Groups metrics into families and indicates whether values are safely additive across dimensions.'
);
