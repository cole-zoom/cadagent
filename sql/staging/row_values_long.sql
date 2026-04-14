CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.stg.row_values_long` (
    staging_value_id    STRING NOT NULL,
    department_id       STRING NOT NULL,
    document_id         STRING NOT NULL,
    table_id            STRING NOT NULL,
    source_row_number   INT64 NOT NULL,
    source_column_number INT64 NOT NULL,
    row_label_raw       STRING,
    header_id           STRING,
    header_raw          STRING,
    value_raw           STRING,
    value_numeric_guess FLOAT64,
    value_date_guess    DATE,
    unit_raw            STRING,
    created_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY department_id, table_id
OPTIONS (
    description = 'Wide raw source tables converted into long row-based format before canonical mapping.'
);
