CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.raw.extracted_tables` (
    table_id                STRING NOT NULL,
    document_id             STRING NOT NULL,
    table_index             INT64 NOT NULL,
    page_number             INT64,
    sheet_name              STRING,
    table_title_raw         STRING,
    table_subtitle_raw      STRING,
    section_title_raw       STRING,
    extraction_method       STRING NOT NULL,
    parser_version          STRING NOT NULL,
    extraction_confidence   FLOAT64,
    gcs_uri                 STRING,
    created_at              TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY document_id
OPTIONS (
    description = 'Metadata about each table extracted from a raw source document.'
);
