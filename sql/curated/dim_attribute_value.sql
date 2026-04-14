CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_attribute_value` (
  attribute_value_id STRING    NOT NULL,
  attribute_type_id  STRING    NOT NULL,
  value_en           STRING,
  value_fr           STRING,
  normalized_value   STRING,
  created_at         TIMESTAMP
)
OPTIONS (
  description = 'Dimension table for individual attribute values belonging to an attribute type. Stores bilingual display values and a normalized key for consistent joins and grouping.'
);
