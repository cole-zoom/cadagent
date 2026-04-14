CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_attribute_type` (
  attribute_type_id   STRING    NOT NULL,
  attribute_type_name STRING    NOT NULL,
  description         STRING,
  created_at          TIMESTAMP
)
OPTIONS (
  description = 'Dimension table defining the types of extensible attributes (e.g., program activity, transfer payment type) that can be attached to observations without altering the core schema.'
);
