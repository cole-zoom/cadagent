CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.raw.extracted_cells` (
    cell_id             STRING NOT NULL,
    table_id            STRING NOT NULL,
    row_number          INT64 NOT NULL,
    column_number       INT64 NOT NULL,
    header_raw          STRING,
    value_raw           STRING,
    value_type_guess    STRING,
    unit_raw            STRING,
    note_flag           BOOL DEFAULT FALSE,
    created_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY table_id
OPTIONS (
    description = 'Cell-level output from extracted tables before normalization. Used for debugging and reprocessing.'
);
