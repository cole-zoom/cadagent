CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_geography` (
  geography_id STRING    NOT NULL,
  geo_type     STRING,
  code         STRING,
  name_en      STRING,
  name_fr      STRING,
  created_at   TIMESTAMP
)
OPTIONS (
  description = 'Dimension table for geographic entities such as provinces, territories, regions, and national-level aggregates. Provides bilingual names and a type hierarchy.'
);
