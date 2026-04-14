# trace-ca — Work Tracking

## Phase 0: Repository Bootstrap
- [x] Directory skeleton
- [x] pyproject.toml
- [x] .gitignore, .env.example
- [x] README.md with architecture flowchart

## Phase 1: Shared Foundations + Ingestion + Raw DDL
- [ ] shared/config/ (settings, logging)
- [ ] shared/clients/ (gcs, bigquery, goc_api)
- [ ] shared/utils/hashing.py
- [ ] shared/models/document.py
- [ ] sql/raw/ DDL (documents, extracted_tables, extracted_cells)
- [ ] services/ingest/ (config, run, main, Dockerfile)
- [ ] infra/terraform/ (main, variables, outputs)
- [ ] infra/cloud_run/ingest.yaml
- [ ] scripts/backfill_department.py

## Phase 2: Extraction + Parsers + Staging DDL
- [ ] shared/models/table.py
- [ ] shared/utils/text_normalization.py
- [ ] shared/utils/time_parsing.py
- [ ] sql/staging/ DDL (headers, row_values_long)
- [ ] services/extract/parsers/ (csv, xlsx, xml, html)
- [ ] services/extract/ (config, run, main, Dockerfile)

## Phase 3: Normalization + Mappings + Curated DDL
- [ ] sql/curated/ DDL (all dim + fact tables)
- [ ] mappings/ YAML dictionaries
- [ ] services/normalize/classifiers/
- [ ] services/normalize/mappers/
- [ ] services/normalize/transforms/
- [ ] scripts/seed_mappings.py

## Phase 4: Quality + Review Queue
- [ ] quality.observation_quality DDL
- [ ] Quality scoring
- [ ] Review queue CLI
- [ ] Quality views

## Phase 5: Agent API
- [ ] services/agent_api/ (main, prompts, sql_validator, citation)
