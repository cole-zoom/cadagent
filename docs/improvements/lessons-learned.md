# Lessons Learned — 2026-04-15 POC sprint

Mistakes, root causes, fixes, and prevention. Captured end-of-day so the next
iteration doesn't repeat them.

---

## 1. Normalization crashed on every table (`unhashable type: 'dict'`)

**What happened**
First normalization run errored on all 2,274 tables. The original log line
(`logger.error("Error normalizing table %s: %s", ..., e)`) printed only the
exception message, not the traceback, so "unhashable type: 'dict'" gave no
clue where it was happening.

**Root cause**
`services/normalize/classifiers/header_classifier.py::_load_junk_patterns`
iterated YAML entries and passed each directly to `re.compile()`. The YAML
entries are dicts (`{pattern, source, note}`), not bare strings, so `re.compile`
got a dict and blew up when it hashed the input for its cache.

**Fix applied**
- Extract `entry["pattern"]` when entry is a dict
- Changed `logger.error` → `logger.exception` so tracebacks surface

**How to prevent**
- **Log with `logger.exception` on unexpected errors** — never swallow tracebacks
  in broad `except` blocks
- **Add a YAML schema test** that loads each mapping file at startup and
  asserts each entry matches the expected shape
- **Never trust `isinstance` over schema validation** — if a YAML file *could*
  contain dicts or strings, be explicit about which

---

## 2. Normalization produced observations for only 2.4% of tables

**What happened**
First successful run: 43,336 observations across 54 of 2,274 tables. StatCan
produced zero. Three compounding causes.

**Root causes**

1. **Extraction garbage**: CSV parser passed every column through even if the
   header was an empty string. 9,868 empty headers in Finance alone. Also, ZIP
   files with `format=csv` metadata reached `_decode` which uses Latin-1
   fallback (decodes any bytes), producing `PK`-prefixed "headers".

2. **StatCan transform expected SDMX shape** (`@OBS_VALUE`, `@TIME_PERIOD`) but
   the actual data was census-wide (`DAUID2006`, `Population, 2016`) or
   taxonomy/classification tables.

3. **Finance transform assumed "metric label col 0 + time-as-columns"** but the
   common shape was dimensional-long with `CATEGORY`/`TAX`/`SUBJECT`/`ITEM`/
   `VALUE` columns.

**Fixes applied**
- Binary-content guard in extract + magic-byte sniff in ingest
- Empty-header-column filter in csv/xlsx/html parsers
- StatCan router with three sub-paths (SDMX long, census-wide, taxonomy)
- Finance `_transform_dimensional_long` triggered by `VALUE` column presence

Result: 708,932 observations (16× improvement), StatCan 0 → 660,830.

**How to prevent**
- **Look at actual data before writing transforms** — don't assume shape from
  documentation. Run a "what does the data actually look like" query on raw
  headers before shipping a strategy.
- **Integration tests against real fixtures** — save a handful of real CSVs/
  XMLs from each department as test fixtures. Parser changes should be
  validated against them, not synthetic test data.
- **Always guard binary at text-parser boundaries** — Latin-1 "works" on any
  bytes. If you're about to decode, magic-byte check first.
- **Empty-header columns are always garbage** — this should have been default
  behavior on day one

---

## 3. BigQuery `DELETE` fails when rows are still in the streaming buffer

**What happened**
`reprocess_extract.py --department statcan` errored:
> UPDATE or DELETE statement over table X would affect rows in the streaming
> buffer, which is not supported

Meaning 30-90 minutes of waiting before the reprocess could proceed.

**Root cause**
BigQuery streaming inserts (`insertAll` / `insert_rows_json`) land rows in a
streaming buffer that is not modifiable via DML for up to 90 minutes. The
extract service uses streaming inserts for BQ writes, so any recently-run
extraction creates a buffer that blocks DELETE.

**Fix applied**
- `reprocess_extract.py` now uses `TRUNCATE TABLE` when scope is `--all`
  (TRUNCATE works on streaming buffer)
- Split the script into `--wipe-only` + `--no-wipe` phases so wiping can run
  once and extraction can run in parallel per-department afterward

**How to prevent**
- **Use BQ load jobs instead of streaming inserts for bulk writes** — already
  documented in `pipeline-optimization.md §2.1`. Load jobs commit directly and
  can be modified by DML immediately. Stream only for true event-at-a-time
  workloads.
- **Scripts that DELETE should also have a TRUNCATE path** — never assume the
  buffer will be flushed when a user runs a reprocess
- **Hybrid threshold** already exists in extract (streaming below 2000 rows,
  load job above). Normalize should adopt the same pattern.

---

## 4. Cloud Build test failure from a stale HTML parser test

**What happened**
Added empty-header-column filtering to `parse_html`. Cloud Build's test step
failed because `test_uneven_rows_padded` expected headers padded to the widest
row (`["A", "B", ""]`) but now the empty-header column is dropped.

**Root cause**
Changed behavior in production code without updating the corresponding test.
Test was green before the change, red after.

**Fix applied**
Renamed the test to `test_uneven_rows_drop_empty_header_columns` with updated
assertions. The new behavior is actually more correct — empty-header columns
carry no analytic meaning.

**How to prevent**
- **Run the full test suite locally before pushing**, not just the slice for
  the code you're touching
- **Pre-commit hook that runs pytest** on the affected paths
- **When changing parser behavior, grep tests for related assertions** as part
  of the change

---

## 5. Local tests broke because `.env` was polluted with shell exports

**What happened**
`pytest` failed during collection with:
> `region` Extra inputs are not permitted
> `service` Extra inputs are not permitted
> `image` Extra inputs are not permitted

Pydantic-settings loaded the `.env` file and rejected unknown keys.

**Root cause**
I wrote in the deploy guide:
```bash
export PROJECT_ID=duwillagence
export REGION=us-east1
export SERVICE=agent-api
export IMAGE=...
```
The user pasted those lines into `.env` instead of their shell. Pydantic with
default `extra="forbid"` rejected `SERVICE` and `IMAGE` as unknown fields.
A later addition of `ANTHROPIC_API_KEY` to `.env` broke things again, because
that key is consumed directly by the anthropic SDK, not pydantic.

**Fixes applied**
- Removed the stray exports from `.env`
- Changed `Settings.model_config` to `extra="ignore"` so unrelated env vars
  (like `ANTHROPIC_API_KEY` consumed by other SDKs) don't break settings
  loading

**How to prevent**
- **Default all settings models to `extra="ignore"`** — `.env` files are
  shared with other tools, not pristine. Breaking because of a key another
  library uses is brittle.
- **Be explicit in docs**: shell commands go in the shell, not `.env`. Prefix
  shell-only examples with `# shell (not .env):`
- **Commit a `.env.example`** with only the keys this project reads, so it's
  obvious what belongs there

---

## 6. Agent returned no results because curated dimension tables were empty

**What happened**
Deployed agent API. Every query returned 0 rows. Inspection showed:
- `dim_document`: 0 rows
- `dim_time`: 0 rows
- `dim_metric`: 51 rows but only 1 had a canonical_name populated
- `dim_geography`: 22 rows (from static YAML)

Meanwhile `fact_observation` had 708K rows with valid IDs pointing at these
empty dim tables.

**Root cause**
The normalize service writes `fact_observation` rows with deterministic hash
IDs for metric / time / document / geography. It never writes the
corresponding `dim_*` rows. The mapping_resolver generates the ID but doesn't
create a dim entry with a canonical label.

The agent's system prompt tells the LLM to JOIN dim_document for provenance
and dim_time for readable time periods. Every LLM-generated query INNER JOINed
empty tables and returned nothing.

**Fixes applied**
- Backfilled `dim_document` from `raw.documents`
  (`WHERE document_id IN (SELECT DISTINCT document_id FROM fact_observation)`)
- Wrote `scripts/populate_dim_time.py` — re-classifies staging headers through
  `HeaderClassifier`, extracts time labels, regenerates the time_id hash
  (deterministic) and inserts one dim_time row per label. 31 dim_time rows.
- **Not fixed**: `dim_metric` still only has "Population" as a named metric —
  most queries that reference specific metric names (GDP, unemployment,
  revenues) will fail to match.

**How to prevent**
- **Write dim entries when you write fact entries** — the `MappingResolver`
  should upsert into `dim_*` whenever it generates a new hash ID. Even an
  unresolved metric should get a `dim_metric` row with the raw header as
  `canonical_name` (low quality, but non-null).
- **End-to-end smoke test** — after normalize runs, assert
  `COUNT(DISTINCT time_id FROM fact_observation) == COUNT(*) FROM dim_time`
  and the same for metric, geography, document. CI should fail if a dim is
  empty.
- **Populate dim_document during ingest**, not at query time — `raw.documents`
  already has everything; duplicate it into `cur.dim_document` as the last
  ingest step.

---

## 7. Wrong `gcloud builds submit` flags in deploy guide

**What happened**
Wrote `gcloud builds submit --tag $IMAGE --project $PROJECT_ID -f services/agent_api/Dockerfile .`
User ran it, got:
> argument --tag/-t: expected one argument

**Root causes**
- `$IMAGE` wasn't set (user in a fresh shell)
- `-f` isn't a valid flag for `gcloud builds submit` — that's a Docker flag.
  Cloud Build gets the Dockerfile via `--config cloudbuild.yaml` or a default
  `Dockerfile` at source root.

**Fix applied**
Rewrote the deploy guide to use the existing `cloudbuild.yaml`:
```bash
gcloud builds submit --config cloudbuild.yaml --project duwillagence
```

**How to prevent**
- **Check existing build infra before writing custom commands** — the repo
  already had a full `cloudbuild.yaml` I didn't notice
- **Never write shell examples without running them first** — the `-f` flag
  wouldn't have shipped if I'd validated it
- **Prefer project-shipped automation over ad-hoc commands in docs** — point
  at the committed `cloudbuild.yaml` rather than reinventing the build

---

## 8. Docker builds on ARM Mac rejected by Cloud Run

**What happened**
`build_and_push.sh` built native ARM images. Cloud Run is amd64-only and
rejects them silently (image runs, service fails health check with no clear
error).

**Root cause**
Missing `--platform linux/amd64` on `docker build`.

**Fix applied**
Added `--platform linux/amd64` to `build_and_push.sh`. Already present in
`cloudbuild.yaml`, so using Cloud Build avoids this entirely.

**How to prevent**
- **Scripts should default to amd64 on Mac** — the common deploy target is
  Linux x86
- **Reuse `cloudbuild.yaml` for local builds too** — `gcloud builds submit`
  runs on Google's Linux builders and avoids platform mismatch
- **CI should include an "image runs on amd64" smoke test** — pull the image,
  run `--platform linux/amd64`, check health endpoint

---

## 9. Frontend textarea had no visual affordance

**What happened**
First render of the web page: users couldn't tell where to type. The
placeholder text rendered in the same serif display font as the hero heading,
so "What was Canada's real GDP growth in 2023?" looked like content, not a
prompt. No visible input border, no cursor.

**Root cause**
Aesthetic-first design with zero affordance for interactivity. The textarea
had `bg-transparent` and no border; the button was at 30% opacity when
disabled, reading as decorative.

**Fix applied**
- Added "YOUR QUESTION" small-caps label above the input
- Bottom border that darkens on focus
- `autoFocus` so the cursor is present on page load
- Active button state: solid ink box with paper-colored text, hovers to
  accent red

**How to prevent**
- **Test UIs with someone who hasn't seen the design brief** — they'll tell
  you instantly what's unclickable
- **Every interactive element needs an affordance** — border, underline,
  cursor, color, or motion. "Brutalist minimalism" is not an excuse for
  mystery meat nav.
- **Default form fields to having a visible boundary**; strip it later if the
  aesthetic demands

---

## Top themes across all of the above

1. **Trust actual data, not documentation.** Four of the nine bugs traced
   back to assumptions about data shape that real data didn't match.
2. **Guard boundaries.** Binary content at text parsers, empty headers,
   streaming buffers, pydantic extras — every boundary between systems needs
   an explicit check.
3. **Never swallow stack traces.** One `logger.error` → `logger.exception`
   change would have saved hours.
4. **Close the loop between writes and reads.** If you write a hash ID,
   write the dim entry. If you change parser behavior, update the test.
5. **Validate your own instructions.** Shell commands in docs should be
   tested end-to-end before being handed to a user.
