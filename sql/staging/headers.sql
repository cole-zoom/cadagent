CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.stg.headers` (
    header_id                   STRING NOT NULL,
    department_id               STRING NOT NULL,
    document_id                 STRING NOT NULL,
    table_id                    STRING NOT NULL,
    header_raw                  STRING NOT NULL,
    header_normalized           STRING NOT NULL,
    header_language             STRING,
    header_class                STRING,
    classification_confidence   FLOAT64,
    first_seen_at               TIMESTAMP NOT NULL
)
PARTITION BY DATE(first_seen_at)
CLUSTER BY department_id, header_class
OPTIONS (
    description = 'Every distinct raw header found during extraction, normalized for classification and mapping.'
);
