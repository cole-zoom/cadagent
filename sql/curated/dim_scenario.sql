CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_scenario` (
  scenario_id    STRING    NOT NULL,
  scenario_name  STRING    NOT NULL,
  scenario_group STRING,
  created_at     TIMESTAMP
)
OPTIONS (
  description = 'Dimension table for budget and planning scenarios (e.g., baseline, optimistic, pessimistic). Allows observations to be tagged with the assumptions under which they were produced.'
);
