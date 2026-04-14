# CADagent: Government of Canada Data Platform

A data platform that ingests Government of Canada open data (Finance, StatCan, TBS-SCT), extracts tables from raw files, normalizes messy headers into canonical dimensions, and loads clean facts into BigQuery. An agent API layer answers natural-language questions with SQL and source provenance.

## Architecture

```mermaid
flowchart TB
    A[Government of Canada API] --> B[Cloud Run Ingestion Job]

    B --> C[GCS Raw Bucket<br/>raw files: pdf xlsx csv json]
    B --> D[BigQuery Metadata Tables<br/>documents ingest status provenance]

    C --> E[Cloud Run Extraction Job]
    E --> F[GCS Processed Bucket<br/>extracted text tables parquet json]
    E --> G[BigQuery Staging Tables<br/>raw_headers document_tables parsed rows]

    G --> H[Cloud Run Normalization Job]
    F --> H

    H --> I[BigQuery Curated Dimensions<br/>dim_metric dim_time dim_geography dim_scenario]
    H --> J[BigQuery Fact Tables<br/>fact_observation]

    K[User Question] --> L[Agent / API Layer]
    L --> M[LLM SQL Generation]
    M --> N[BigQuery Query Execution]
    N --> O[Answer]

    J --> N
    I --> N

    O --> P[Optional Source Citation Lookup]
    P --> D
    P --> F
    P --> C
```

## Data Sources

| Department | Code | Data Types |
|-----------|------|------------|
| Finance Canada | `fin` | Federal budgets, fiscal monitors, economic forecasts, transfer tables |
| Statistics Canada | `statcan` | SDMX time series (labour, GDP, CPI, trade, demographics) |
| Treasury Board Secretariat | `tbs-sct` | Estimates, PSES surveys, proactive disclosure, COVID expenditures |

## Tech Stack

- **Language**: Python 3.12+
- **Orchestration**: Cloud Run jobs (one per pipeline stage)
- **Storage**: GCS (raw files + extracted artifacts)
- **Warehouse**: BigQuery (raw -> staging -> curated)
- **IaC**: Terraform
- **Agent**: FastAPI + Claude for NL-to-SQL

## Repository Structure

```
services/              # One sub-package per pipeline stage
  ingest/              # GoC API -> GCS + raw.documents
  extract/             # Raw files -> parsed tables, headers, cells
  normalize/           # Header classification + mapping -> curated dims/facts
  agent_api/           # NL question -> SQL -> answer + provenance
shared/                # Cross-service code
  config/              # Settings, logging
  clients/             # GCS, BigQuery, GoC API wrappers
  models/              # Document, table, observation dataclasses
  utils/               # Text normalization, time parsing, hashing
mappings/              # YAML dictionaries (metrics, geography, scenario, junk)
sql/                   # DDL + transform SQL by layer (raw/, staging/, curated/)
infra/                 # Cloud Run YAMLs + Terraform
scripts/               # One-off backfill / seed / reprocess helpers
docs/                  # Architecture and schema docs
external/              # Source data fixtures and schema inventories
```

## BigQuery Layers

| Layer | Tables | Purpose |
|-------|--------|---------|
| **raw** | `documents`, `extracted_tables`, `extracted_cells` | Source metadata and extraction artifacts |
| **stg** | `headers`, `header_mapping_candidates`, `row_values_long` | Parsed but not yet canonical staging data |
| **cur** | `dim_department`, `dim_document`, `dim_metric`, `dim_time`, `dim_geography`, `dim_scenario`, `dim_attribute_type`, `dim_attribute_value`, `fact_observation`, `bridge_observation_attribute` | Clean warehouse tables the agent queries |
| **quality** | `observation_quality` | Quality scores and review metadata |

## Pipeline Stages

1. **Ingest** -- Pull metadata + files from GoC CKAN API, dedup by `source_url` + checksum, land in GCS, write `raw.documents`
2. **Extract** -- Detect file type, route to parser (PDF/XLSX/CSV/HTML/XML), emit `raw.extracted_tables`, `stg.headers`, `stg.row_values_long`
3. **Normalize** -- Classify headers (metric/time/geo/scenario/attribute/junk), resolve mappings, pivot wide-to-long, load curated dims + `fact_observation`
4. **QA / Serve** -- Flag low-confidence mappings, expose provenance, support agent queries

## Setup

### Prerequisites

- Python 3.12+
- Google Cloud SDK (`gcloud`)
- Terraform
- A GCP project with BigQuery and Cloud Storage APIs enabled

### Local Development

```bash
# Clone and install
git clone <repo-url>
cd trace-ca
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Copy and configure environment
cp .env.example .env
# Edit .env with your GCP project details

# Run DDL to create BigQuery tables
python scripts/seed_mappings.py

# Run ingestion for a department
python services/ingest/main.py
```

### Deploy Infrastructure

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

## Design Docs

| Document | Path | Describes |
|----------|------|-----------|
| Repo layout | `docs/repo_schema/goc_repo_schema.md` | Target directory tree |
| Pipeline + mapping | `docs/pipeline_schema/goc_pipeline_and_mapping_plan.md` | Four pipeline stages, mapping philosophy |
| BigQuery schema | `docs/warehouse_schema/goc_bigquery_schema_summary.md` | Every table across all layers |
| CKAN API guide | `docs/external_api_guide/ckan-api-guide.md` | GoC open data API reference |
