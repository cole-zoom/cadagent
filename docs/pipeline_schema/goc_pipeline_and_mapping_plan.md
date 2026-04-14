# GoC Data Pipelines Plan + Header Mapping Design

## Goal

Build a cheap, maintainable pipeline stack that:

- pulls raw Government of Canada documents from the API
- stores raw files in GCS
- extracts tables/text from those files
- normalizes messy headers into canonical concepts
- loads clean, queryable data into BigQuery
- supports an agent that answers natural-language questions with SQL + source provenance

This document focuses on **how to build the pipelines** and **how mapping should work**.

---

# 1. Pipeline architecture

There should be **four main pipeline stages**:

1. **Ingestion**
2. **Extraction**
3. **Normalization**
4. **Serving / QA support**

Each stage should be its own Cloud Run job or service, but all should live in the same monorepo.

---

# 2. Pipeline 1 — Ingestion

## Purpose
Pull source metadata and files from the Government of Canada API and land them in raw storage.

## Input
- Government of Canada API
- department filters
- source metadata returned by the API

## Output
- raw files in GCS
- document metadata in BigQuery

## What it does
1. Query the GoC API by department
2. Enumerate available documents
3. Check whether each document is new or already ingested
4. Download raw file
5. Write raw file to GCS
6. Insert or update `raw.documents` in BigQuery

## Recommended GCS path
`gs://<bucket>/raw/goc/department=<dept_code>/year=<yyyy>/document_id=<id>/<filename>`

## BigQuery tables touched
- `raw.documents`

## Key logic
- dedupe using `source_url` + checksum
- assign a stable `document_id`
- never overwrite raw files silently
- track ingestion status

## Suggested job cadence
- scheduled daily or weekly
- manual backfill mode for historical loads

---

# 3. Pipeline 2 — Extraction

## Purpose
Turn raw files into machine-readable table/text artifacts.

## Input
- raw files from GCS
- document metadata from BigQuery

## Output
- extracted tables/text artifacts in GCS
- extracted table metadata in BigQuery
- raw headers in BigQuery
- raw cell/value rows in BigQuery staging

## What it does
1. Read raw file from GCS
2. Detect file type: PDF, XLSX, CSV, HTML, etc.
3. Route to correct extractor
4. Extract:
   - document text
   - table boundaries
   - raw headers
   - row-level values
5. Write extracted artifacts to GCS
6. Write extraction metadata and raw parsed outputs to BigQuery staging

## GCS output paths
- `processed/text/...`
- `processed/tables/...`
- `processed/json/...`

## BigQuery tables touched
- `raw.extracted_tables`
- `stg.headers`
- `stg.row_values_long`

## Key logic
- keep parser version
- keep extraction confidence
- preserve row/column coordinates when possible
- do not normalize yet

## Important rule
Extraction should preserve source truth, not interpret meaning.

---

# 4. Pipeline 3 — Normalization

## Purpose
Convert parsed raw table content into canonical dimensions and facts.

## Input
- staging rows
- raw headers
- mapping dictionaries
- classification rules

## Output
- curated dimensions
- curated fact rows
- mapping audit records

## What it does
1. Normalize raw header strings
2. Classify headers into semantic buckets
3. Resolve header mappings to canonical entities
4. Parse time-like headers into `dim_time`
5. Parse geography headers into `dim_geography`
6. Parse scenario headers into `dim_scenario`
7. Convert wide tables into long observations
8. Load `cur.fact_observation`
9. Attach provenance fields and quality scores

## BigQuery tables touched
- `stg.header_classification`
- `stg.header_mapping_candidates`
- `cur.dim_metric`
- `cur.dim_time`
- `cur.dim_geography`
- `cur.dim_scenario`
- `cur.dim_attribute_type`
- `cur.dim_attribute_value`
- `cur.fact_observation`
- `cur.bridge_observation_attribute`

## Key logic
- one raw header may map to one canonical entity
- some raw headers are time/geography/scenario, not metrics
- one observation can have multiple attribute slices
- keep all mappings explainable

---

# 5. Pipeline 4 — QA / review / serving support

## Purpose
Support human review, quality checks, and agent provenance.

## Input
- curated facts
- mapping outputs
- extraction confidence
- validation rules

## Output
- data quality records
- review queues
- source citation paths

## What it does
1. Flag low-confidence mappings
2. Flag rows with conflicting units or impossible values
3. Flag unmapped headers
4. Store quality notes
5. Expose provenance for agent answers

## Tables touched
- `stg.header_mapping_candidates`
- optional review queue table
- optional observation quality table

---

# 6. How mapping should work

This is the most important part of the whole system.

You are **not** mapping raw headers directly to database columns.

You are mapping raw headers to **semantic roles** and then to **canonical entities**.

---

# 7. Mapping philosophy

Every raw header should go through these stages:

1. **Normalize text**
2. **Classify header type**
3. **Generate candidate mappings**
4. **Pick best mapping**
5. **Store mapping decision**
6. **Use mapping in normalization**

This should be deterministic where possible, and reviewable when ambiguous.

---

# 8. Step 1 — Normalize header text

## Goal
Reduce meaningless variation before classification.

## Example transformations
- trim whitespace
- lowercase
- normalize Unicode dashes
- remove repeated spaces
- remove line breaks
- standardize `%`, `$`, `M$`
- normalize accented vs unaccented variants only if safe
- strip obvious noise suffixes when rule-based and safe

## Examples
- `Change(%)` → `change (%)`
- `Variation (%)` → `variation (%)`
- `2023–2024` → `2023-2024`
- `U.S. CPI Inflation` → `us cpi inflation`
- `Transferts\nfédéraux` → `transferts fédéraux`

## Output field
- `header_normalized`

---

# 9. Step 2 — Classify header type

## Goal
Before asking “what metric is this?”, first ask “what kind of thing is this?”

## Allowed classes
- `metric`
- `time`
- `time_range`
- `geography`
- `scenario`
- `attribute`
- `unit`
- `junk`

## Examples
- `2024-25` → `time`
- `April to December 2023-24` → `time_range`
- `Canada` → `geography`
- `Projection` → `scenario`
- `Women` → `attribute`
- `CPI Inflation` → `metric`
- `@code` → `junk`

## How to classify
Use a layered approach:
1. regex rules
2. lookup dictionaries
3. exact matches
4. fuzzy/embedding fallback
5. manual review if unresolved

## Output table
- `stg.header_classification`

### Suggested fields
- `header_id`
- `header_class`
- `confidence`
- `classification_method`

---

# 10. Step 3 — Generate candidate mappings

Only after classification.

## For `metric` headers
Generate candidate `metric_id` matches

Examples:
- `revenus` → `total_revenue`
- `real gdp growth` → `gdp_real_growth`
- `debt charges` → `public_debt_charges`

## For `time` headers
Generate candidate `time_id` or parse-time values

Examples:
- `2023-24` → fiscal year 2023-04-01 to 2024-03-31
- `January 2024` → month 2024-01-01 to 2024-01-31

## For `geography`
Map to province/country/territory IDs

Examples:
- `ON` → Ontario
- `Canada` → Canada
- `U.S.` → United States

## For `scenario`
Map to scenario dimension

Examples:
- `Actual`
- `Projection`
- `Baseline`
- `High`
- `Low`

## For `attribute`
Map to flexible attribute values

Examples:
- `Women`
- `Men`
- `Services`
- `Age 65+`

## Output table
- `stg.header_mapping_candidates`

### Suggested fields
- `header_id`
- `canonical_entity_type`
- `canonical_entity_id`
- `candidate_score`
- `mapping_method`
- `approved`

---

# 11. Step 4 — Pick the mapping

## Rules
- if exact match exists and confidence is high → auto-approve
- if strong regex/parse match exists for time → auto-approve
- if multiple candidates are close → send to review
- if no match → leave unmapped and flag

## Auto-approve examples
- `2024-2025` parsed to fiscal year
- `canada` exact geography match
- `projection` exact scenario match

## Review-required examples
- `change`
- `total`
- `measure`
- `services`
- `expenses` when context is unclear

---

# 12. Step 5 — Store mapping decisions permanently

## Why
Mappings should improve over time.

Once you review and approve a mapping, future runs should reuse it.

## Suggested persistent mapping table
`meta.header_mapping_rules`

### Fields
- `mapping_rule_id`
- `header_normalized`
- `header_class`
- `canonical_entity_type`
- `canonical_entity_id`
- `department_scope` (nullable)
- `document_type_scope` (nullable)
- `language`
- `priority`
- `is_active`
- `created_at`
- `created_by`

## Why scope matters
Some mappings are global:
- `canada`
- `projection`

Some are department-specific:
- `program expenses`
- `vote 1`
- `statistical measure labels`

---

# 13. Step 6 — Use context, not just header text

Header text alone is often not enough.

Mapping should use context from:
- department
- document type
- table title
- nearby headers
- units
- source language

## Example
`Total` by itself is meaningless.

But:
- in a finance table with nearby `Revenues` and `Expenses`, it may be a fiscal total
- in a survey table, it may be a population total
- in a region table, it may be a geography aggregate

## Recommendation
Candidate generation should consider:
- `header_normalized`
- `table_title`
- `document_type`
- `department_id`
- sibling headers in same table

---

# 14. Step 7 — Convert wide tables into long observations

After mapping, translate source tables into facts.

## Example source table
| Metric | 2023-24 | 2024-25 |
|---|---:|---:|
| Revenues | 100 | 110 |

## Output observations
- metric = `total_revenue`, time = `2023-24`, value = 100
- metric = `total_revenue`, time = `2024-25`, value = 110

## Principle
Time-like headers become rows, not columns.

---

# 15. Mapping priority order

Use this order to keep mapping robust:

1. exact normalized rule match
2. scoped rule match (department + document type)
3. parser rule (time/scenario/geography)
4. dictionary synonym match
5. fuzzy string match
6. embedding similarity
7. manual review

Do not jump straight to embeddings.

---

# 16. What should be hard-coded vs learned

## Hard-code
- fiscal year parsing rules
- month parsing rules
- province/country codes
- common scenario values
- obvious junk headers
- unit parsing rules

## Dictionary-driven
- metric synonyms
- bilingual label mappings
- department-specific canonical terms

## Review-driven
- ambiguous business terms
- overloaded words like `total`, `change`, `measure`
- new department-specific jargon

---

# 17. Quality controls for mapping

Every normalization run should produce:

## Counts
- total headers seen
- auto-mapped headers
- review-required headers
- unmapped headers

## Warnings
- ambiguous mappings
- conflicting unit detections
- duplicate candidate matches
- impossible time parses

## Success metrics
- % headers classified
- % headers mapped
- % observations loaded
- % observations with full provenance

---

# 18. Minimal MVP mapping workflow

For v1, keep it simple.

## MVP approach
1. normalize text
2. classify with rules
3. parse time/geography/scenario with deterministic logic
4. map common metrics using a synonym dictionary
5. put unresolved headers into review queue
6. rerun normalization with approved mappings

This is enough to get real progress without overengineering.

---

# 19. Recommended repo structure for pipelines

```text
repo/
  services/
    ingest/
    extract/
    normalize/
    agent_api/
  shared/
    config/
    utils/
    schemas/
    mapping/
      metric_dictionary.yaml
      geography_dictionary.yaml
      scenario_dictionary.yaml
      junk_headers.yaml
  sql/
    raw/
    staging/
    curated/
  infra/
    cloud_run/
    terraform/
```

## Why
- each pipeline is independently deployable
- shared mapping logic stays centralized
- easy to keep one source of truth

---

# 20. Recommended rollout plan

## Phase 1
- build ingestion
- load raw docs to GCS
- write `raw.documents`

## Phase 2
- build extraction
- write raw headers + row values
- preserve provenance

## Phase 3
- implement normalization rules
- build classification + mapping tables
- populate dimensions and facts

## Phase 4
- add review queue
- improve synonym dictionaries
- add quality scoring

## Phase 5
- wire agent to curated BigQuery tables
- add citation fallback to GCS / extracted artifacts

---

# 21. Final recommendation

Your pipeline should be designed around this idea:

> **Raw files are preserved in GCS.  
> Raw extracted structures land in staging.  
> Mapping converts messy headers into canonical entities.  
> Canonical entities power BigQuery facts and dimensions.**

And your mapping system should be:

- rule-first
- context-aware
- reviewable
- reusable
- incremental

That will keep the system cheap, understandable, and extendable.
