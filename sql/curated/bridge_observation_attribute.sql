CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.bridge_observation_attribute` (
  observation_id     STRING    NOT NULL,
  attribute_value_id STRING    NOT NULL,
  created_at         TIMESTAMP
)
CLUSTER BY observation_id
OPTIONS (
  description = 'Bridge table implementing a many-to-many relationship between observations and extensible attribute values, allowing each observation to carry an arbitrary set of descriptive tags without widening the fact table.'
);
