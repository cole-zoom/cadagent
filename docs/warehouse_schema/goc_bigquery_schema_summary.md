# Government of Canada Data Platform — BigQuery Schema Summary

This document describes the recommended BigQuery schema in plain English.

It is organized by layer:
- **Raw**: metadata about source documents and extracted source artifacts
- **Staging**: parsed-but-not-yet-canonical data used during normalization
- **Curated**: clean warehouse tables that the agent should query

The actual raw files live in **GCS**. BigQuery stores the **metadata, parsed structures, and canonical tables**.

---

## 1) `raw.documents`

### What it holds
A queryable catalog of every source document ingested from the Government of Canada APIs.

The actual file lives in GCS, but this table lets you filter, join, and trace facts back to the source document.

### Headers
- `document_id` — unique ID for the document in your system
- `department_id` — normalized department identifier
- `department_code` — short department code if available
- `gcs_uri` — path to the raw file in GCS
- `source_url` — original download URL from the source API
- `title` — document title
- `document_type` — report, budget, table package, annual report, etc.
- `file_format` — pdf, csv, xlsx, html, json, etc.
- `language` — English, French, bilingual, or detected language
- `published_date` — publication date if available
- `effective_date` — effective date if distinct from publication date
- `fiscal_year_label` — raw fiscal year label from source if present
- `checksum` — hash of the file for deduplication
- `source_system` — API or source family the document came from
- `ingested_at` — timestamp when your system ingested the file
- `ingestion_status` — success, failed, skipped, duplicate, etc.

---

## 2) `raw.extracted_tables`

### What it holds
Metadata about each table extracted from a raw source document.

One document can produce many extracted tables.

### Headers
- `table_id` — unique ID for the extracted table
- `document_id` — foreign key to `raw.documents`
- `table_index` — ordinal position of the table within the document
- `page_number` — page number for PDFs, if applicable
- `sheet_name` — worksheet name for spreadsheets, if applicable
- `table_title_raw` — raw table title or caption
- `table_subtitle_raw` — raw subtitle if present
- `section_title_raw` — surrounding section title if captured
- `extraction_method` — parser or extraction strategy used
- `parser_version` — parser version used for extraction
- `extraction_confidence` — extraction confidence score
- `gcs_uri` — path to extracted table artifact in GCS if stored separately
- `created_at` — timestamp table metadata was created

---

## 3) `raw.extracted_cells`

### What it holds
Cell-level output from extracted tables before normalization.

This is optional, but very useful for debugging and reprocessing.

### Headers
- `cell_id` — unique cell ID
- `table_id` — foreign key to `raw.extracted_tables`
- `row_number` — row position in the raw extracted table
- `column_number` — column position in the raw extracted table
- `header_raw` — raw header associated with the cell if known
- `value_raw` — original raw value as extracted
- `value_type_guess` — guessed type such as text, numeric, date, percent
- `unit_raw` — raw unit text if detected
- `note_flag` — whether the cell appears to be a note or footnote
- `created_at` — timestamp row was created

---

## 4) `stg.headers`

### What it holds
Every distinct raw header found during extraction, normalized enough for classification and mapping.

This is the key table for turning messy source headers into a stable warehouse schema.

### Headers
- `header_id` — unique ID for the header record
- `department_id` — department that produced the source table
- `document_id` — source document ID
- `table_id` — source table ID
- `header_raw` — original header text exactly as extracted
- `header_normalized` — cleaned header text with spacing, dash, and punctuation normalized
- `header_language` — detected language of the header
- `header_class` — metric, time, geography, scenario, attribute, unit, junk
- `classification_confidence` — confidence score for the header class
- `first_seen_at` — first time this header was seen in the pipeline

---

## 5) `stg.header_mapping_candidates`

### What it holds
Candidate mappings from raw headers to canonical entities.

This is used during normalization and review.

### Headers
- `header_id` — foreign key to `stg.headers`
- `canonical_entity_type` — metric, time, geography, scenario, attribute_type, attribute_value
- `canonical_entity_id` — proposed canonical ID
- `candidate_method` — rule, dictionary, embedding, manual, etc.
- `candidate_score` — confidence score for this mapping candidate
- `approved_flag` — whether the candidate is approved for production use
- `approved_by` — reviewer or system process that approved it
- `approved_at` — timestamp of approval

---

## 6) `stg.row_values_long`

### What it holds
A staging table where wide raw source tables are converted into a long row-based format before canonical mapping.

This is one of the most useful intermediate tables.

### Headers
- `staging_value_id` — unique staging record ID
- `department_id` — source department
- `document_id` — source document
- `table_id` — source table
- `source_row_number` — original row number
- `source_column_number` — original column number
- `row_label_raw` — raw row label if present
- `header_id` — related header ID
- `header_raw` — raw header text for convenience
- `value_raw` — original value from the source
- `value_numeric_guess` — parsed numeric value if possible
- `value_date_guess` — parsed date value if possible
- `unit_raw` — raw unit text
- `created_at` — timestamp the staging row was created

---

## 7) `cur.dim_department`

### What it holds
The canonical list of departments.

This lets facts across departments use a consistent identifier.

### Headers
- `department_id` — unique canonical department ID
- `department_code` — short code
- `department_name_en` — English department name
- `department_name_fr` — French department name
- `active_flag` — whether the department is currently active in your catalog
- `created_at` — timestamp created

---

## 8) `cur.dim_document`

### What it holds
A cleaned, warehouse-friendly document dimension used by analytics and the agent.

This is the curated version of document metadata.

### Headers
- `document_id` — unique document ID
- `department_id` — foreign key to `cur.dim_department`
- `title` — cleaned title
- `document_type` — canonical document type
- `language` — canonical language value
- `published_date` — cleaned publication date
- `fiscal_year_label` — normalized fiscal year label if present
- `source_url` — original source URL
- `gcs_uri` — raw file URI in GCS
- `parser_version` — parser version used for the current canonical load
- `created_at` — timestamp created

---

## 9) `cur.dim_metric`

### What it holds
The canonical metric dictionary.

This is one of the most important tables. It defines what a number means.

### Headers
- `metric_id` — unique canonical metric ID
- `canonical_name` — standard metric name in English
- `canonical_name_fr` — standard metric name in French
- `metric_family` — fiscal, macroeconomic, labour, program, survey, tax, etc.
- `default_unit_id` — default canonical unit identifier
- `description` — plain-language definition of the metric
- `is_additive` — whether it makes sense to sum this metric across rows
- `created_at` — timestamp created

---

## 10) `cur.dim_time`

### What it holds
The canonical time dimension.

This is how you avoid keeping years, months, quarters, and ranges as separate source columns.

### Headers
- `time_id` — unique canonical time ID
- `time_type` — year, fiscal_year, quarter, month, date, range
- `label` — normalized display label
- `start_date` — start date of the time period
- `end_date` — end date of the time period
- `fiscal_year_start_month` — start month of the fiscal year if relevant
- `is_projection` — whether the period is projected rather than actual
- `created_at` — timestamp created

---

## 11) `cur.dim_geography`

### What it holds
The canonical geography dimension.

This is used when the same metrics are broken down by country, province, territory, or region.

### Headers
- `geography_id` — unique geography ID
- `geo_type` — country, province, territory, region, city, etc.
- `code` — code such as ON, QC, CA, US if applicable
- `name_en` — English geography name
- `name_fr` — French geography name
- `created_at` — timestamp created

---

## 12) `cur.dim_scenario`

### What it holds
The canonical list of scenario labels attached to observations.

Useful for actual vs projection vs baseline vs upside/downside.

### Headers
- `scenario_id` — unique scenario ID
- `scenario_name` — actual, projection, baseline, high, low, upside, downside, etc.
- `scenario_group` — broader grouping of scenarios if needed
- `created_at` — timestamp created

---

## 13) `cur.dim_attribute_type`

### What it holds
A catalog of flexible slice dimensions that do not deserve their own dedicated dimension table.

Examples: gender, age group, sector, organization, measure type, beneficiary type.

### Headers
- `attribute_type_id` — unique attribute type ID
- `attribute_type_name` — name of the slice dimension
- `description` — optional description
- `created_at` — timestamp created

---

## 14) `cur.dim_attribute_value`

### What it holds
The allowed canonical values for each flexible slice dimension.

Examples: `gender = women`, `age_group = 25-54`, `sector = services`.

### Headers
- `attribute_value_id` — unique attribute value ID
- `attribute_type_id` — foreign key to `cur.dim_attribute_type`
- `value_en` — English value
- `value_fr` — French value
- `normalized_value` — normalized internal form
- `created_at` — timestamp created

---

## 15) `cur.fact_observation`

### What it holds
The core analytical fact table.

Each row is one observation: one metric, for one time period, possibly for one geography and one scenario, sourced from one document and one table.

This is the main table your agent should query.

### Headers
- `observation_id` — unique fact row ID
- `department_id` — foreign key to `cur.dim_department`
- `document_id` — foreign key to `cur.dim_document`
- `table_id` — source table ID
- `metric_id` — foreign key to `cur.dim_metric`
- `time_id` — foreign key to `cur.dim_time`
- `geography_id` — foreign key to `cur.dim_geography`, nullable
- `scenario_id` — foreign key to `cur.dim_scenario`, nullable
- `value_numeric` — numeric value if the observation is numeric
- `value_text` — text value if the observation is non-numeric
- `unit_raw` — raw unit text from the source if preserved
- `scale_factor` — multiplier such as 1, 1000, 1000000
- `currency_code` — CAD, USD, etc. if relevant
- `source_row_number` — row number in the extracted source table
- `source_column_number` — column number in the extracted source table
- `quality_score` — confidence or quality score for the final observation
- `created_at` — timestamp created

---

## 16) `cur.bridge_observation_attribute`

### What it holds
A bridge table that lets one fact observation carry many additional slice dimensions.

This makes the schema extensible without having to add new columns every time a department introduces a new type of categorical breakdown.

### Headers
- `observation_id` — foreign key to `cur.fact_observation`
- `attribute_value_id` — foreign key to `cur.dim_attribute_value`
- `created_at` — timestamp created

---

## 17) `quality.observation_quality`

### What it holds
Optional quality and review metadata for fact observations.

Useful for debugging, manual review, and agent trust scoring.

### Headers
- `observation_id` — foreign key to `cur.fact_observation`
- `quality_confidence` — confidence score for the observation
- `issue_codes` — list or encoded set of issue types
- `issue_notes` — free-text notes about data quality issues
- `review_status` — unreviewed, reviewed, approved, rejected
- `reviewed_by` — reviewer or system process
- `reviewed_at` — timestamp reviewed

---

# Recommended use by layer

## The agent should primarily query
- `cur.fact_observation`
- `cur.dim_metric`
- `cur.dim_time`
- `cur.dim_department`
- `cur.dim_geography`
- `cur.dim_scenario`
- `cur.bridge_observation_attribute`
- `cur.dim_attribute_value`
- `cur.dim_attribute_type`

## The pipeline should primarily write to
- `raw.documents`
- `raw.extracted_tables`
- `raw.extracted_cells`
- `stg.headers`
- `stg.header_mapping_candidates`
- `stg.row_values_long`
- curated tables after normalization

## GCS should store
- raw source files
- extracted text artifacts
- extracted table artifacts
- intermediate parquet/json outputs

---

# Design principles behind this schema

- Raw files live in **GCS**, not BigQuery
- BigQuery stores **metadata, staging outputs, and canonical analytical tables**
- Time is modeled as a **dimension**, not as source columns
- Headers are treated as **messy source signals**, not warehouse schema
- The fact table is **long and extensible**
- Additional slices are handled by the **attribute bridge**, so new departments do not force schema rewrites

