# Pipeline Optimization: Cloud Run Fit + Recurring Scheduling

The current pipelines work locally but hit Cloud Run's constraints: streaming inserts are slow, large files cause timeouts, and there's no retry/resume logic. This document details how to fix each pipeline and set up recurring scheduling once the initial data load is complete.

---

## Current Bottlenecks

| Pipeline | Problem | Impact |
|----------|---------|--------|
| Ingest | Downloads files sequentially, one at a time | Slow — 784 files took ~40 min |
| Ingest | Large StatCan files (400MB+) block for 20 min each | Single file can eat the whole timeout |
| Extract | Streaming inserts of 500 rows at a time to BQ | Each insert is an HTTP round trip — thousands of them per document |
| Extract | Parses entire file in memory | OOM risk on large files in Cloud Run (2GB limit) |
| Extract | No resume — crash means re-processing from scratch | Wastes work on restart |
| Normalize | Reconstructs the entire table grid from row_values_long | Reads back what extract just wrote — slow and redundant |
| All | No parallelism | Each document processed sequentially |

---

## Pipeline 1: Ingest Improvements

### 1.1 Parallel downloads with asyncio

Replace the sequential download loop with async downloads. The GoC API doesn't rate-limit hard, and most time is spent waiting for HTTP responses.

```python
import asyncio
import aiohttp

async def download_batch(urls: list[str], max_concurrent: int = 10) -> list[tuple[str, bytes]]:
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        async def fetch(url):
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    return url, await resp.read()
        return await asyncio.gather(*[fetch(u) for u in urls], return_exceptions=True)
```

**Impact**: 5-10x faster ingestion. 784 files in ~5 min instead of 40.

**Cloud Run fit**: Easily within 1-hour timeout.

### 1.2 Batch BQ inserts

Instead of inserting one document row at a time, buffer 50-100 rows and insert in a single call.

```python
buffer = []
for resource in resources:
    # ... download and process ...
    buffer.append(record.to_bq_row())
    if len(buffer) >= 50:
        bq_client.insert_rows(raw_dataset, "documents", buffer)
        buffer = []
if buffer:
    bq_client.insert_rows(raw_dataset, "documents", buffer)
```

**Impact**: 15x fewer BQ API calls.

### 1.3 Pre-filter with HEAD requests

Before downloading, send a HEAD request to check Content-Length and Content-Type. This is faster than the current approach of starting a GET with `stream=True`.

```python
async def should_download(session, url) -> bool:
    async with session.head(url, allow_redirects=True, timeout=10) as resp:
        content_length = int(resp.headers.get("Content-Length", 0))
        content_type = resp.headers.get("Content-Type", "")
        if content_length > 100 * 1024 * 1024:
            return False
        if "zip" in content_type or "pdf" in content_type:
            return False
        return True
```

**Impact**: Instantly skip large/unsupported files without even starting the download.

### 1.4 GCS parallel uploads

Use `google-cloud-storage`'s built-in concurrent upload or upload in a thread pool:

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(gcs_client.upload_raw_file, ...) for ...]
    for future in futures:
        future.result()
```

**Impact**: Overlaps uploads with downloads.

---

## Pipeline 2: Extract Improvements

### 2.1 Replace streaming inserts with load jobs

This is the single biggest improvement. Streaming inserts (`insert_rows_json`) have per-row overhead. Load jobs (`load_table_from_json`) are bulk operations that handle millions of rows in seconds.

```python
import json
import io

def bulk_insert(bq_client, dataset, table, rows: list[dict]):
    """Use a load job instead of streaming insert."""
    ndjson = "\n".join(json.dumps(row) for row in rows)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
    job = bq_client.client.load_table_from_file(
        io.BytesIO(ndjson.encode()),
        f"{bq_client.project_id}.{dataset}.{table}",
        job_config=job_config,
    )
    job.result()  # Wait for completion
```

**Impact**: A document with 50,000 row_values_long entries goes from 100 streaming insert calls (500 rows each, ~2 sec per call = 200 sec) to 1 load job (~5 sec).

### 2.2 Write to GCS then load

For very large tables, write NDJSON to GCS first, then load from GCS. This avoids the 10MB request body limit on load jobs.

```python
def bulk_insert_via_gcs(bq_client, gcs_client, dataset, table, rows, bucket):
    ndjson = "\n".join(json.dumps(row) for row in rows)
    gcs_uri = gcs_client.upload_processed_file(
        bucket, f"staging/{dataset}/{table}/{uuid4()}.ndjson", ndjson.encode(), "application/json"
    )
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq_client.client.load_table_from_uri(gcs_uri, f"...", job_config=job_config)
    job.result()
```

**Impact**: No size limit. Can load millions of rows from a single file.

### 2.3 Track extraction progress per-document

Add an `extraction_status` column to `raw.documents` (or a separate tracking table) so the extract service knows exactly which documents succeeded, failed, or are in-progress.

```sql
ALTER TABLE `raw.documents` ADD COLUMN extraction_status STRING DEFAULT 'pending';
-- Values: pending, in_progress, completed, failed
```

Then the batch query becomes:

```sql
SELECT * FROM raw.documents
WHERE ingestion_status = 'success' AND extraction_status = 'pending'
LIMIT 100
```

**Impact**: True resume on crash. No wasted reprocessing. Can also limit batch size to stay within Cloud Run timeout.

### 2.4 Batch size control

Process N documents per Cloud Run execution instead of all of them:

```python
BATCH_SIZE = int(os.getenv("EXTRACT_BATCH_SIZE", "100"))

sql = f"""
    SELECT * FROM raw.documents
    WHERE extraction_status = 'pending'
    LIMIT {BATCH_SIZE}
"""
```

Run the job multiple times (via scheduler) and each execution chews through 100 documents. If a single execution takes 20 min, that's well within the 1-hour timeout.

**Impact**: Guaranteed Cloud Run timeout compliance.

### 2.5 Stream-parse large files

Instead of loading entire CSVs into memory, use streaming parsers:

```python
import csv

def parse_csv_streaming(data: bytes, document_id: str) -> Iterator[list[str]]:
    text = _decode(data)
    reader = csv.reader(io.StringIO(text, newline=""))
    headers = next(reader)
    for row in reader:
        yield row
```

**Impact**: Constant memory usage regardless of file size. Allows processing 100MB+ files in Cloud Run's 2GB memory.

---

## Pipeline 3: Normalize Improvements

### 3.1 Avoid re-reading from BQ

Currently normalize reads headers and row_values_long back from BQ to reconstruct the table grid. This is slow and redundant — extract just wrote that data.

**Option A**: Have extract write the parsed table as a Parquet/JSON file to GCS, and normalize reads from GCS directly.

```python
# In extract, after parsing:
table_json = json.dumps({"headers": table.headers, "rows": table.rows})
gcs_uri = gcs_client.upload_processed_file(
    bucket, f"tables/{table.table_id}.json", table_json.encode()
)

# In normalize:
table_data = json.loads(gcs_client.download_file(gcs_uri))
```

**Option B**: Use BigQuery EXPORT to dump the relevant rows to GCS as NDJSON, then read locally.

**Impact**: Eliminates the grid reconstruction step. Normalize becomes 5-10x faster.

### 3.2 Batch dimension lookups

Currently each header resolution does individual lookups. Pre-load all existing dimension values at the start of a normalization batch:

```python
existing_metrics = {row["canonical_name"]: row["metric_id"]
                    for row in bq_client.query("SELECT * FROM cur.dim_metric")}
existing_times = {row["label"]: row["time_id"]
                  for row in bq_client.query("SELECT * FROM cur.dim_time")}
```

**Impact**: No per-header BQ queries. One query per dimension table at startup.

### 3.3 Bulk load fact_observation

Same as extract — use load jobs instead of streaming inserts for fact_observation.

**Impact**: Massive speedup on large normalization runs.

### 3.4 Add normalization status tracking

Same pattern as extract — track which tables have been normalized:

```sql
ALTER TABLE `raw.extracted_tables` ADD COLUMN normalization_status STRING DEFAULT 'pending';
```

**Impact**: True resume. Batch size control.

---

## Scheduling: Recurring Pipeline Runs

Once the initial data load is complete, the pipeline needs to run periodically to pick up new documents published by the GoC.

### Architecture

```
Cloud Scheduler
    |
    | (cron trigger)
    v
Cloud Workflows (orchestrator)
    |
    |-- Step 1: Execute ingest-dev job (--wait)
    |-- Step 2: Execute extract-dev job (--wait)
    |-- Step 3: Execute normalize-dev job (--wait)
    |-- Step 4: (optional) Notify on failure
    v
Done
```

### Option A: Cloud Scheduler + Cloud Workflows (recommended)

Cloud Workflows orchestrates the three jobs sequentially, handles retries, and can send alerts on failure.

#### Enable APIs

```bash
gcloud services enable \
  cloudscheduler.googleapis.com \
  workflows.googleapis.com \
  workflowexecutions.googleapis.com
```

#### Create the workflow

```yaml
# infra/workflows/pipeline.yaml
main:
  steps:
    - init:
        assign:
          - project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          - region: "us-east1"

    - run_ingest:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: ${"projects/" + project + "/locations/" + region + "/jobs/ingest-dev"}
        result: ingest_execution

    - wait_ingest:
        call: googleapis.run.v2.projects.locations.jobs.executions.get
        args:
          name: ${ingest_execution.metadata.name}
        result: ingest_result

    - run_extract:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: ${"projects/" + project + "/locations/" + region + "/jobs/extract-dev"}
        result: extract_execution

    - wait_extract:
        call: googleapis.run.v2.projects.locations.jobs.executions.get
        args:
          name: ${extract_execution.metadata.name}
        result: extract_result

    - run_normalize:
        call: googleapis.run.v2.projects.locations.jobs.run
        args:
          name: ${"projects/" + project + "/locations/" + region + "/jobs/normalize-dev"}
        result: normalize_execution

    - done:
        return: "Pipeline complete"
```

#### Deploy the workflow

```bash
gcloud workflows deploy trace-pipeline \
  --source=infra/workflows/pipeline.yaml \
  --location=us-east1
```

#### Schedule it

```bash
# Run every Sunday at 3 AM ET
gcloud scheduler jobs create http trace-pipeline-weekly \
  --location=us-east1 \
  --schedule="0 8 * * 0" \
  --time-zone="UTC" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/duwillagence/locations/us-east1/workflows/trace-pipeline/executions" \
  --http-method=POST \
  --oauth-service-account-email=trace-pipeline@duwillagence.iam.gserviceaccount.com
```

### Option B: Simple shell script + Cloud Scheduler (simpler)

If you don't want Cloud Workflows, use the existing `run_full_pipeline.sh` triggered by Cloud Scheduler calling a thin Cloud Run service that executes it.

Or just run it manually when you want new data:

```bash
./scripts/run_full_pipeline.sh
```

### Scheduling frequency considerations

| Frequency | When to use |
|-----------|-------------|
| Weekly | GoC publishes most datasets monthly or quarterly. Weekly catches everything with minimal cost. |
| Daily | Only if you need near-real-time data (unlikely for government fiscal data). |
| Monthly | Fine for initial phase. Most GoC data updates quarterly. |
| Manual | During development. Run when you want, check results, iterate. |

**Recommendation**: Start with manual runs. Once the pipeline is stable, move to weekly. The incremental ingest mode means each run only downloads new/updated files — a weekly run with no new data takes < 1 minute.

---

## Implementation Priority

| Priority | Change | Effort | Impact |
|----------|--------|--------|--------|
| 1 | Replace streaming inserts with load jobs (extract + normalize) | 2-3 hours | 10-50x faster BQ writes |
| 2 | Add extraction/normalization status tracking | 1 hour | True resume, batch control |
| 3 | Batch size control on extract + normalize | 30 min | Cloud Run timeout compliance |
| 4 | Async downloads in ingest | 2 hours | 5-10x faster ingestion |
| 5 | GCS intermediate files for normalize | 2 hours | Eliminates grid reconstruction |
| 6 | Cloud Workflows orchestration | 1 hour | Automated scheduling |
| 7 | Stream-parse large files | 3 hours | Handles 100MB+ files in 2GB memory |

### Quick wins (do these first)

Items 1-3 alone will make all three pipelines fit comfortably within Cloud Run's constraints. Total effort: ~4 hours.

### After quick wins

The pipeline can run on Cloud Run with:
- Ingest: processes all departments in < 30 min
- Extract: processes 100 documents per execution, runs multiple times if needed
- Normalize: processes 100 tables per execution, same pattern

Then set up Cloud Workflows (item 6) to chain them on a weekly schedule.

---

## Cost Impact

These optimizations also reduce costs:

| Change | Cost impact |
|--------|------------|
| Load jobs vs streaming inserts | Streaming inserts cost $0.01/200MB. Load jobs are free. |
| Batch size control | Shorter Cloud Run executions = less CPU billing |
| Skip large files early | Less GCS storage, less network egress |
| Weekly vs daily scheduling | 4x fewer Cloud Run executions per month |

---

## Lessons Learned (from initial data load)

Real issues hit during the first end-to-end run. Each one resulted in a code fix. Documented here so we don't repeat them.

### Ingest

| Issue | Root cause | Fix applied |
|-------|-----------|-------------|
| 20-min silent hangs during ingestion | Downloading 400-500MB StatCan CSVs, no progress log | Added per-resource download logging and `Content-Length` header check before downloading body |
| CKAN metadata lies about format | URLs ending in `.zip` reported as `format: CSV` | Added URL extension check alongside metadata format filter |
| Downloading French duplicates | GoC publishes every file in EN and FR | Filter to `language == "en"` only + skip `-fra.*` URL patterns |
| Downloading unsupported formats | HTML, PDF, ZIP files ingested but can't be parsed | Added `SUPPORTED_FORMATS` allowlist checked before download |
| `$PROJECT_ID` empty in scripts | Env var not exported or lost between terminal sessions | Always run from repo root so `.env` is found by pydantic-settings |
| YAML `ON:` parsed as boolean `True` | YAML spec treats bare `ON`/`OFF`/`YES`/`NO` as booleans | Quoted `"ON":` in geography YAML + `str()` coercion in `_make_id` |

### Extract

| Issue | Root cause | Fix applied |
|-------|-----------|-------------|
| BQ streaming insert timeout (600s) | Thousands of 500-row streaming inserts per large document | Hybrid approach: streaming for < 2000 rows, load jobs for larger |
| `Infinity` in JSON payload | `float("inf")` from parsing edge-case cell values | Added `math.isfinite()` check in `_try_parse_numeric` |
| CSV parse error on newlines in fields | GoC CSVs have unquoted newlines in cell values | Added `newline=""` in StringIO + fallback to `splitlines()` |
| Load jobs slow for small tables | Load job startup overhead (~30-60s) worse than streaming for tiny tables | Threshold at 2000 rows — streaming below, load job above |

### Normalize

| Issue | Root cause | Fix applied |
|-------|-----------|-------------|
| `department_id not found in et` | SQL referenced `et.department_id` but column is on `documents` table | Changed filter to `d.department_id` |
| `LIMIT 1000` on batch query | Arbitrary cap meant normalization required multiple manual reruns | Removed the limit — process all pending tables in one run |
| No per-table progress logging | Normalization appeared to hang with no output between "Found N tables" and final summary | Added per-table start/complete logs with index, table_id, and observation counts |
| `unhashable type: 'dict'` on every table | `_load_junk_patterns` passed whole YAML dict entries (`{pattern, source, note}`) straight into `re.compile()` instead of extracting the `pattern` field | Extract `entry["pattern"]` when entry is a dict; also swapped `logger.error` → `logger.exception` so tracebacks actually surface |
| StatCan: 0/226 tables produced observations | Transform only handled SDMX-ML `@OBS_VALUE`/`@TIME_PERIOD` shape, but actual data is census-wide (`Population, 2016` / `DAUID2006`) or classification taxonomy | Split `StatcanTransformStrategy` into a router with three sub-paths: SDMX/CANSIM long-format, census-wide (year embedded in header), and taxonomy (empty). Census-wide uses a keyword-guarded year regex to avoid misclassifying geo-ID columns. See `data-quality-fixes.md` |
| Finance: 38/1621 tables produced observations | Transform assumed "metric label col 0 + time columns" but actual data is dimensional-long with a dedicated `VALUE` column and categorical columns (CATEGORY, TAX, SUBJECT, ITEM, SUBITEM) | Added `_transform_dimensional_long` path triggered by presence of VALUE/amount/montant column. Composite metric label from `metric_label_columns` in new `mappings/finance_dimensions.yaml`. Classic wide-format path remains as fallback |
| ZIP files reached the CSV parser as garbage headers (`PK ¸h8E`) | `_decode` falls back to Latin-1 which decodes any bytes, and no magic-byte check existed | Added `_is_binary_content` guard in `extract/run.py` that returns early for `PK`/`%PDF`/`\x1f\x8b`/etc. prefixes. Also added a ZIP sniff in `ingest/run.py` to avoid persisting binary to GCS |
| 9,868 empty-string headers in Finance `stg.headers` | CSV/XLSX/HTML parsers only trimmed trailing empty columns; interior empties flowed through | All three parsers now drop any column whose header is empty/whitespace-only after padding |

### General

| Issue | Root cause | Fix applied |
|-------|-----------|-------------|
| `bq query` syntax errors | BigQuery requires backtick-quoted `project.dataset.table` refs, escaped as `\`` in shell | Updated all BQ commands in deploy guide |
| `uv sync` removes packages | Optional dependencies not installed by default | Added `[dependency-groups] dev` that pulls in all extras |
| Docker images rejected by Cloud Run | Built on ARM Mac, Cloud Run needs `linux/amd64` | Added `--platform linux/amd64` to all docker build commands |
| `gcloud run jobs execute` wrong flag | `--set-env-vars` not valid on `execute`, only on `deploy` | Changed to `--update-env-vars` |
