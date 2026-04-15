# Pipeline Data-Quality Fixes: Three-Problem Implementation Plan

Initial normalization run produced 43,336 observations across only 54 of 2,274 tables (2.4% table success rate). Breakdown by department:

| department | tables_total | tables_with_obs | total_obs |
|------------|--------------|-----------------|-----------|
| fin        | 1621         | 38              | 38,823    |
| tbs-sct    | 455          | 16              | 4,513     |
| statcan    | 226          | 0               | 0         |

Root causes are in extraction (garbage inputs) and in two of the three transform strategies. This plan addresses all three.

---

## Problem 1 — Extraction Produces Garbage Headers

### 1.1 Root Cause

Two distinct sub-issues creating garbage in `stg.headers`.

**Sub-issue A: Empty-string headers pass through extraction unchecked.**

`services/extract/parsers/csv.py:49` assigns `headers = all_rows[0]`. Lines 52-55 strip trailing empty columns, but there is no filtering of *interior* empty-string headers. If the source CSV's first row contains cells like `"","CATEGORY","","TAX","","VALUE"`, the empty strings become headers. They flow into `stg.headers` and `row_values_long`. Same applies in `services/extract/parsers/xlsx.py:57` and `services/extract/parsers/html.py:47-48`.

**Sub-issue B: ZIP files disguised as CSV reach the CSV parser.**

`services/ingest/run.py:107-108` checks URL extensions for `.zip`, `.gz`, etc. and skips those. However, the GoC CKAN API sometimes serves ZIP files from URLs that don't end in `.zip`. The `SUPPORTED_FORMATS` allowlist checks the metadata `format` field, not actual file contents. When a ZIP arrives with `format: CSV`, it gets stored and `services/extract/run.py:114-126` routes it to `parse_csv`, whose `_decode` function (line 90-97) always succeeds because `latin-1` can decode any byte sequence. ZIP magic bytes like `PK` become "headers".

### 1.2 Proposed Fix

**Fix A: Add binary-content detection at the start of extraction.**

In `services/extract/run.py`, add a guard function:

```python
BINARY_MAGIC_BYTES = {
    b"PK": "zip",           # ZIP / XLSX (XLSX handled separately)
    b"\x1f\x8b": "gzip",
    b"%PDF": "pdf",
    b"\xd0\xcf\x11\xe0": "ole2",  # Old .xls, .doc
    b"Rar!": "rar",
}

def _is_binary_content(data: bytes, file_format: str) -> bool:
    if file_format in ("xlsx", "xls"):
        return False
    prefix = data[:8]
    for magic, fmt in BINARY_MAGIC_BYTES.items():
        if prefix.startswith(magic):
            return True
    sample = data[:512]
    non_printable = sum(1 for b in sample if b < 0x20 and b not in (0x09, 0x0A, 0x0D))
    return non_printable / max(len(sample), 1) > 0.10
```

Call at the top of `extract_document` before `_route_to_parser`. If `True`, log a warning and return `{"tables": 0, ...}`.

**Fix B: Filter empty and degenerate headers during extraction.**

In `services/extract/parsers/csv.py`, after line 49:

```python
valid_col_indices = [i for i, h in enumerate(headers) if h.strip()]
if len(valid_col_indices) < len(headers):
    headers = [headers[i] for i in valid_col_indices]
    data_rows = [[row[i] if i < len(row) else None for i in valid_col_indices] for row in data_rows]
```

Same pattern in `xlsx.py` (after line 57) and `html.py` (after line 47).

**Fix C (defense-in-depth): Content-type sniff at ingest time.**

In `services/ingest/run.py`, after line 127:

```python
if data[:2] == b"PK" and file_format == "csv":
    logger.warning("ZIP magic bytes in file declared as CSV: %s", resource_url)
    stats["skipped"] += 1
    continue
```

### 1.3 Risks

- Binary guard: near-zero false-positive risk. The 10% non-printable threshold is conservative; Latin-1 accented characters are in 0x80-0xFF, not below 0x20.
- Empty header filtering changes column count of extracted tables. Already-extracted tables will have a different schema than newly-extracted ones. Requires reprocessing (section 6).
- No BQ schema changes.

### 1.4 Verification

```sql
SELECT header_raw, COUNT(*) as cnt
FROM `PROJECT.stg.headers`
WHERE department_id = 'statcan' AND header_raw LIKE 'PK%'
GROUP BY 1;
-- Expected: 0 rows

SELECT COUNT(*) FROM `PROJECT.stg.headers` WHERE TRIM(header_raw) = '';
-- Expected: 0
```

Tests: `test_zip_bytes_return_empty`, `test_empty_header_columns_filtered`, `test_binary_content_detection`.

---

## Problem 2 — StatCan Transform Doesn't Match Actual Data

### 2.1 Root Cause

`services/normalize/transforms/wide_to_long.py:178-238` — `StatcanTransformStrategy.transform` looks exclusively for SDMX column names: `@obs_value`, `value`, `@time_period`, `ref_date`, `@geography`, `geo`. Hard early-return at line 200 if these aren't present.

Actual StatCan data on open.canada.ca has three shapes:

1. **Census-style wide tables**: Headers like `DAUID2006`, `POP2006`, `HRNAME2013`, `Population, 2016`, `Population density per square kilometre, 2016`. Year embedded in header name.
2. **CANSIM/NDM long-format CSVs**: Headers like `REF_DATE`, `GEO`, `DGUID`, `VALUE`, `STATUS`. Each row is one observation. Close to SDMX.
3. **Classification/taxonomy tables**: `Level`, `Code`, `Class title`, `Hierarchical structure`. Reference data, not observations.

The current strategy only handles (loosely) shape 2. Shape 1 is the most common and produces zero observations. Shape 3 should correctly produce zero.

### 2.2 Proposed Fix

**A. Convert `StatcanTransformStrategy.transform` into a router:**

```python
def transform(self, headers, rows, department_id, document_id, table_id):
    header_lower = [h.lower().strip() for h in headers]

    # Shape 1: SDMX/CANSIM long format
    obs_value_idx = _find_col(header_lower, ["@obs_value", "value"])
    time_idx = _find_col(header_lower, ["@time_period", "ref_date"])
    if obs_value_idx is not None and time_idx is not None:
        return self._transform_long_format(...)

    # Shape 2: Census-wide (year embedded in column names)
    year_columns = self._detect_year_columns(headers)
    if year_columns:
        return self._transform_census_wide(...)

    # Shape 3: Classification/taxonomy — no observations
    return []
```

**B. Refactor existing SDMX logic into `_transform_long_format`** (move current body of `transform` lines 194-238).

**C. New `_detect_year_columns`:**

```python
YEAR_IN_HEADER_RE = re.compile(r'(19|20)\d{2}')

def _detect_year_columns(self, headers):
    year_cols = []
    for i, h in enumerate(headers):
        match = YEAR_IN_HEADER_RE.search(h)
        if match:
            year = match.group(0)
            h_lower = h.lower()
            if any(kw in h_lower for kw in ("pop", "density", "income", "dwelling",
                "household", "area", "land", "count", "total", "median", "average")):
                year_cols.append((i, year))
    return year_cols
```

**D. New `_transform_census_wide`:** Unpivot census-wide tables. For each row, identify the geo-id column (DAUID/HRUID/CSDUID/etc.), then emit one observation per (row, year-column) pair. Derive metric label from header minus the year (e.g., `"Population, 2016"` → metric=`"Population"`, time=`2016`).

**E. Add census metric synonyms to `mappings/metric_dictionary.yaml`:**

```yaml
census_population:
  canonical_name_en: "Census Population"
  synonyms: ["population", "pop", "population count"]

census_population_density:
  canonical_name_en: "Population Density"
  default_unit: per_sq_km
  synonyms: ["population density per square kilometre", "density"]

census_dwellings:
  canonical_name_en: "Private Dwellings"
  synonyms: ["total private dwellings", "private dwellings", "dwellings"]
```

### 2.3 Risks

- Year-in-header detection uses a keyword allowlist to distinguish population columns from geo-ID columns that also contain years (e.g., `DAUID2006`). Conservative start; expand based on empirical results.
- CANSIM long-format should work once Problem 1 is fixed (ZIP-as-CSV issue likely masked this).
- Classification/taxonomy tables correctly produce zero. No action.

### 2.4 Verification

```sql
SELECT COUNT(*) as obs_count, COUNT(DISTINCT table_id) as table_count
FROM `PROJECT.cur.fact_observation`
WHERE department_id = 'statcan';
-- Expected: obs_count > 0, table_count > 0
```

Tests: `test_statcan_cansim_long_format`, `test_statcan_census_wide`, `test_statcan_taxonomy_returns_empty`.

---

## Problem 3 — Finance Transform Assumes Wide Format, but Data is Dimensional Long

### 3.1 Root Cause

`services/normalize/transforms/wide_to_long.py:37-175` — `FinanceTransformStrategy` has two branches (time-as-columns, geo-as-columns), both assuming "first column is metric label."

Actual Finance top headers:

```
CATEGORY | TAX | GROUP | MEASURE | DETAILS | SUBJECT | ITEM | SUBITEM | VALUE | 2016 | ... | 2023 | Projection
```

This is *dimensional long-format*. Dimensional columns (CATEGORY, TAX, GROUP, etc.) are neither time nor geography. The metric is a combination of SUBJECT + ITEM + SUBITEM. The value is in a dedicated `VALUE` column (sometimes with year-columns as a hybrid).

Current strategy:
- Finds time columns (years).
- Falls into `_transform_time_as_columns`.
- Treats row[0] (`CATEGORY` value) as metric label — meaningless.
- The 38 tables that succeed are edge cases where row[0] happens to match a metric synonym.

### 3.2 Proposed Fix

**A. Shape detection in `FinanceTransformStrategy.transform`:**

```python
header_lower = [h.lower().strip() for h in headers]
value_idx = _find_col(header_lower, ["value", "amount", "montant"])

# Heuristic: dedicated VALUE column = long-format
if value_idx is not None:
    return self._transform_dimensional_long(...)

# Existing branches
if len(geo_columns) > len(time_columns) and len(geo_columns) >= 2:
    return self._transform_geo_as_columns(...)
return self._transform_time_as_columns(...)
```

**B. New `_transform_dimensional_long`:** Each row is already an observation, dimensioned by categorical columns. Build composite metric label from metric-label columns (SUBJECT, ITEM, SUBITEM, MEASURE, DESCRIPTION). Emit one obs per (row, VALUE column) and additionally per (row, year-column) for hybrid tables.

**C. New `mappings/finance_dimensions.yaml`:**

```yaml
dimension_columns:
  - category
  - tax
  - group
  - measure
  - details
  - subject
  - item
  - subitem
  - description
  - organization
  - vote
  - vote number
  - program

metric_label_columns:
  - subject
  - item
  - subitem
  - measure
  - description
  - details
```

Keep column-name knowledge in YAML, consistent with existing mapping approach.

### 3.3 Risks

- Composite metric labels (e.g., `"Personal Income Tax | Federal"`) won't match existing synonyms. Fallback hash-based metric_id at `mapping_resolver.py:169` is acceptable for v1. Follow-up: surface top unresolved labels for dictionary curation.
- The 38 currently-succeeding tables will be re-routed to the new dimensional-long path if they have a VALUE column. Their existing observations must be replaced (section 6).
- No schema changes.

### 3.4 Verification

```sql
SELECT COUNT(*) as obs_count, COUNT(DISTINCT table_id) as table_count
FROM `PROJECT.cur.fact_observation`
WHERE department_id = 'fin';
-- Expected: table_count >> 38, obs_count >> 38,823
```

Tests: `test_fin_dimensional_long_with_value_column`, `test_fin_dimensional_long_value_only`, `test_fin_existing_time_as_columns_still_works`.

---

## 5. Ordering and Dependencies

```
Problem 1 (Extraction garbage)   [must land first — upstream pollution]
    |
    v
Re-extract affected documents
    |
    +--> Problem 2 (StatCan)  ─┐
    |                          ├─ parallel
    +--> Problem 3 (Finance)  ─┘
                |
                v
         Re-normalize all tables
```

1. **Ship Problem 1 fixes** (binary guard + empty header filter + ingest sniff).
2. **Re-extract** StatCan at minimum; optionally Finance to get clean headers.
3. **Ship Problems 2 and 3 in parallel.**
4. **Re-normalize everything.**

## 6. Reprocess Strategy

### Current skip mechanism

`services/normalize/run.py:50-53` — LEFT JOIN + `WHERE fo.table_id IS NULL` skips tables that already have observations. Tables with 0 observations (2,220 of 2,274) will be re-attempted automatically. The 54 tables that succeeded will NOT be re-attempted.

### Recommended: Delete-and-rerun

At this scale (43K observations), simplest and safest approach.

**Create `scripts/reprocess_normalize.py`:**

```python
"""Usage:
    python scripts/reprocess_normalize.py --department fin
    python scripts/reprocess_normalize.py --all
"""
```

Runs `DELETE FROM fact_observation WHERE department_id = @dept`, then calls `normalize_batch`.

**Create `scripts/reprocess_extract.py`** for Problem 1 reprocessing:

```
DELETE FROM stg.row_values_long WHERE department_id = @dept;
DELETE FROM stg.headers WHERE department_id = @dept;
DELETE FROM raw.extracted_tables WHERE document_id IN (
    SELECT document_id FROM raw.documents WHERE department_id = @dept);
-- Then run extract_batch
```

**Full reprocess sequence** after all fixes:

```bash
python scripts/reprocess_extract.py --department statcan
python scripts/reprocess_extract.py --department fin
python scripts/reprocess_normalize.py --all
```

### Why not a flag-based approach?

A `needs_reprocess` column adds schema-migration overhead, requires updating batch queries, and is only valuable for selective reprocessing. For this one-time fundamental transform change, delete-and-rerun is simpler, faster, more auditable. Full reprocess at this scale = minutes, not hours. Consider a `normalization_status` column (per `pipeline-optimization.md` §3.4) for later incremental fixes.

---

## Summary of Files to Create or Modify

| Action | File | What changes |
|--------|------|--------------|
| Modify | `services/extract/run.py` | Add `_is_binary_content` guard, call before `_route_to_parser` |
| Modify | `services/extract/parsers/csv.py` | Filter empty-string header columns |
| Modify | `services/extract/parsers/xlsx.py` | Filter empty-string header columns |
| Modify | `services/extract/parsers/html.py` | Filter empty-string header columns |
| Modify | `services/ingest/run.py` | Add ZIP magic-byte sniff after download |
| Modify | `services/normalize/transforms/wide_to_long.py` | Rewrite `StatcanTransformStrategy` as router + census-wide + long-format; add `_transform_dimensional_long` to `FinanceTransformStrategy` |
| Create | `mappings/finance_dimensions.yaml` | Dimensional + metric-label columns for Finance |
| Modify | `mappings/metric_dictionary.yaml` | Add census demographic metric synonyms |
| Create | `scripts/reprocess_extract.py` | Delete + re-extract for a department |
| Create | `scripts/reprocess_normalize.py` | Delete + re-normalize for a department |
| Modify | `tests/services/extract/parsers/test_csv.py` | Tests for empty headers + binary data |
| Modify | `tests/services/extract/test_run.py` | Test for binary content detection |
| Modify | `tests/services/normalize/transforms/test_wide_to_long.py` | Tests for StatCan census-wide, CANSIM long, Finance dimensional-long |
