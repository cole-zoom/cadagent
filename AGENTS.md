# AGENTS.md — trace-ca

## What this repo is

A data platform that ingests Government of Canada open data (Finance, StatCan, TBS-SCT),
extracts tables from raw files, normalizes messy headers into canonical dimensions, and loads
clean facts into BigQuery. An agent API layer answers natural-language questions with SQL and
source provenance.

## Authoritative docs — read before writing code

| Doc | Path | Describes |
|-----|------|-----------|
| Repo layout | `docs/repo_schema/goc_repo_schema.md` | Target directory tree for the monorepo |
| Pipeline + mapping design | `docs/pipeline_schema/goc_pipeline_and_mapping_plan.md` | Four pipeline stages, mapping philosophy, rollout plan |
| BigQuery schema | `docs/warehouse_schema/goc_bigquery_schema_summary.md` | Every table across raw / stg / cur / quality layers |

## Real source data — column inventories

`external/output/` contains per-department CSVs derived from `external/fixtures/`.
Use these to understand what raw headers actually look like in the wild.

| File | What it shows |
|------|---------------|
| `*_column_counts.csv` | `column_name, file_count, department` — frequency of each raw header |
| `*_schemas.csv` | `signature, kind, file_count, num_columns, columns, example_dataset, example_source, department` — distinct table shapes |

Departments currently covered: **fin**, **statcan**, **tbs-sct**.

## Target repo structure

Follow `docs/repo_schema/goc_repo_schema.md` exactly. Key top-level dirs:

```
services/          # one sub-package per pipeline stage
  ingest/          # GoC API → GCS + raw.documents
  extract/         # raw files → parsed tables, headers, cells
  normalize/       # header classification + mapping → curated dims/facts
  agent_api/       # NL question → SQL → answer + provenance
shared/            # cross-service code
  config/          # settings, logging
  clients/         # gcs, bigquery, goc_api wrappers
  models/          # document, table, observation dataclasses
  utils/           # text_normalization, time_parsing, hashing
mappings/          # YAML dictionaries (metrics, geography, scenario, junk)
sql/               # DDL + transform SQL organised by layer (raw/, staging/, curated/)
infra/             # Cloud Run YAMLs + Terraform
scripts/           # one-off backfill / seed / reprocess helpers
```

## BigQuery layers (abbreviated)

Refer to `docs/warehouse_schema/goc_bigquery_schema_summary.md` for full column lists.

- **raw** — `documents`, `extracted_tables`, `extracted_cells`
- **stg** — `headers`, `header_mapping_candidates`, `row_values_long`
- **cur** — `dim_department`, `dim_document`, `dim_metric`, `dim_time`, `dim_geography`, `dim_scenario`, `dim_attribute_type`, `dim_attribute_value`, `fact_observation`, `bridge_observation_attribute`
- **quality** — `observation_quality`

## Pipeline stages

1. **Ingest** — pull metadata + files from GoC API, dedup by `source_url` + checksum, land in GCS, write `raw.documents`
2. **Extract** — detect file type, route to parser (pdf/xlsx/csv/html), emit `raw.extracted_tables`, `stg.headers`, `stg.row_values_long`
3. **Normalize** — classify headers (metric/time/geo/scenario/attribute/junk), generate mapping candidates, resolve mappings, pivot wide→long, load curated dims + `fact_observation`
4. **QA / Serve** — flag low-confidence mappings, expose provenance, support agent queries

## Mapping rules (critical context)

Headers go through: **normalize text → classify type → generate candidates → pick best → store decision → use in transform**.

Priority order for resolution:
1. Exact normalized rule match
2. Scoped rule (department + doc type)
3. Deterministic parser (time, geography, scenario)
4. Dictionary synonym
5. Fuzzy string
6. Embedding similarity
7. Manual review

Classification buckets: `metric`, `time`, `time_range`, `geography`, `scenario`, `attribute`, `unit`, `junk`.

## Tech stack

- **Language**: Python 3.12+
- **Orchestration**: Cloud Run jobs (one per pipeline stage)
- **Storage**: GCS (raw files + extracted artifacts)
- **Warehouse**: BigQuery (raw → stg → cur)
- **IaC**: Terraform
- **Packaging**: `pyproject.toml` at repo root, per-service `requirements.txt`

## Conventions

- Every service has `main.py` (entrypoint), `config.py` (settings), `Dockerfile`, and a `tests/` dir.
- Shared code lives in `shared/`, never duplicated across services.
- SQL files are the source of truth for table DDL — keep them in `sql/`.
- YAML mapping dictionaries live in `mappings/` and are loaded by the normalize service.
- Use `tasks/todo.md` for work tracking and `tasks/lessons.md` for corrections log.

## Build order

Follow the phased rollout in the pipeline doc (§20):

1. **Phase 1** — `shared/` foundations + `services/ingest/` + `sql/raw/` DDL
2. **Phase 2** — `services/extract/` + parsers + `sql/staging/` DDL
3. **Phase 3** — `services/normalize/` + `mappings/` + `sql/curated/` DDL
4. **Phase 4** — QA tables + review queue
5. **Phase 5** — `services/agent_api/`
