goc-data-platform/
  README.md
  pyproject.toml
  .env.example
  .gitignore

  services/
    ingest/
      main.py
      Dockerfile
      requirements.txt
      config.py
      run.py
      tests/

    extract/
      main.py
      Dockerfile
      requirements.txt
      config.py
      parsers/
        pdf.py
        xlsx.py
        csv.py
        html.py
      tests/

    normalize/
      main.py
      Dockerfile
      requirements.txt
      config.py
      classifiers/
      mappers/
      transforms/
      tests/

    agent_api/
      main.py
      Dockerfile
      requirements.txt
      config.py
      prompts/
      tests/

  shared/
    config/
      settings.py
      logging.py
    clients/
      gcs.py
      bigquery.py
      goc_api.py
    models/
      document.py
      table.py
      observation.py
    utils/
      text_normalization.py
      time_parsing.py
      hashing.py

  mappings/
    metric_dictionary.yaml
    geography_dictionary.yaml
    scenario_dictionary.yaml
    junk_headers.yaml
    attribute_dictionary.yaml

  sql/
    raw/
      documents.sql
      extracted_tables.sql
    staging/
      headers.sql
      row_values_long.sql
    curated/
      dim_department.sql
      dim_document.sql
      dim_metric.sql
      dim_time.sql
      dim_geography.sql
      dim_scenario.sql
      dim_attribute_type.sql
      dim_attribute_value.sql
      fact_observation.sql
      bridge_observation_attribute.sql

  infra/
    cloud_run/
      ingest.yaml
      extract.yaml
      normalize.yaml
      agent_api.yaml
    terraform/
      main.tf
      variables.tf
      outputs.tf

  scripts/
    backfill_department.py
    reprocess_document.py
    seed_mappings.py

  docs/
    architecture.md
    schema.md
    mapping_rules.md