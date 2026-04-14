CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.cur.dim_department` (
  department_id   STRING    NOT NULL,
  department_code STRING,
  department_name_en STRING,
  department_name_fr STRING,
  active_flag     BOOL      DEFAULT TRUE,
  created_at      TIMESTAMP
)
OPTIONS (
  description = 'Dimension table for federal departments and agencies. Each row represents a unique department with bilingual names and an active/inactive flag.'
);
