CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.raw.documents` (
    document_id         STRING NOT NULL,
    department_id       STRING NOT NULL,
    department_code     STRING,
    gcs_uri             STRING NOT NULL,
    source_url          STRING NOT NULL,
    title               STRING,
    document_type       STRING,
    file_format         STRING,
    language            STRING,
    published_date      DATE,
    effective_date      DATE,
    fiscal_year_label   STRING,
    checksum            STRING NOT NULL,
    source_system       STRING DEFAULT 'open.canada.ca',
    ingested_at         TIMESTAMP NOT NULL,
    ingestion_status    STRING NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY department_id, ingestion_status
OPTIONS (
    description = 'Catalog of every source document ingested from GoC APIs. Raw files live in GCS.'
);
