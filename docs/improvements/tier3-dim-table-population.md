# Tier 3 — Fix dim-table emptiness at the source

The POC ships with dim tables partially backfilled by ad-hoc scripts. The
root cause is that the `normalize` pipeline writes `fact_observation` rows
with deterministic hash IDs for `metric_id` / `time_id` / `geography_id` /
`document_id` but never writes the corresponding dim rows. Every query that
INNER JOINs those dim tables returns 0 unless someone has manually backfilled.

This doc is the detailed plan to fix it properly. Treat it as the next
sprint's main work item.

---

## Current symptoms (before any manual backfill)

| Dim table         | Rows | Source of entries today                                     |
| ----------------- | ---- | ----------------------------------------------------------- |
| `dim_metric`      | 51   | Seeded from `metric_dictionary.yaml` synonyms (resolver-side). Only matches when a raw header aligns with a synonym — missed 99% of real data. |
| `dim_time`        | 0    | Resolver generates `time_id` hashes but never writes rows.  |
| `dim_geography`   | 22   | Seeded from `geography_dictionary.yaml`. Missing DAUIDs, HRUIDs, and every geo produced from actual StatCan census data. |
| `dim_document`    | 0    | Resolver has no document concept; `raw.documents` is never copied to `cur.dim_document`. |
| `dim_scenario`    | 0    | Rarely referenced. Can remain empty for now.                |
| `dim_attribute_*` | 0    | Not used by current transforms. Park until attributes return. |

The POC's manual fixes (`populate_dim_time.py`, SQL backfill of `dim_metric`,
`dim_document` INSERT from `raw.documents`) are band-aids. They don't rerun
as data changes.

---

## Design decision: where should dim writes happen?

Three candidate architectures. Recommendation is **B**.

### A. Inline during `MappingResolver.resolve()`

Modify every `_resolve_*` method to upsert into `cur.dim_*` when it generates
a new ID.

- **Pros**: one place, write-through semantics.
- **Cons**: resolver does per-row work and can't batch. Streaming inserts per
  row would explode BQ API calls. Caching + flushing gets messy to reason
  about. Also entangles the resolver with BigQuery I/O (currently
  it's pure in-memory logic).

### B. **RECOMMENDED — Batch flush at end of `normalize_batch`**

Let the resolver keep its in-memory caches (`_time_cache`, `_geo_cache`,
`_metric_cache`). After `normalize_batch` finishes processing all tables,
serialize those caches plus a new document cache into dim tables via bulk
MERGE statements.

- **Pros**: one network trip per dim table, no per-row overhead. Resolver
  stays pure. Idempotent — MERGE ensures no duplicates. Simple to add end-of-
  batch hook.
- **Cons**: if normalize crashes mid-batch, dim tables lag behind fact
  rows. Acceptable because the next run will reconcile.

### C. Separate post-normalize `build_dims` service / job

A fourth Cloud Run job that scans `fact_observation` + staging + `raw.documents`
and rebuilds all dim tables from scratch on every run.

- **Pros**: decoupled, easy to re-run, easy to test.
- **Cons**: reads everything every time. Redundant work on large datasets.
  Adds another thing to schedule. Not idempotent in the same clean way as
  MERGE (would need TRUNCATE-and-reinsert, which has streaming-buffer issues
  we already hit).

---

## Implementation plan — Option B

### 1. Extend `MappingResolver` to track labels

The resolver already caches IDs. Extend each cache to also remember the
canonical label so we can write dim rows at flush time.

```python
# shared/models/dim_cache.py  (new)
@dataclass
class DimEntry:
    entity_id: str
    label: str
    # For time
    time_type: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    is_projection: bool = False
    # For geography
    geo_type: str | None = None
    code: str | None = None
    # For metric
    metric_family: str | None = None
    is_additive: bool = False
    default_unit_id: str | None = None
```

Resolver changes:

- `_resolve_time` populates `self._time_dims[time_id] = DimEntry(..., time_type=parsed.time_type, start_date=...)`
- `_resolve_geography` populates `self._geo_dims[geography_id] = DimEntry(..., geo_type=..., code=...)`. For unresolved geos (like DAUIDs), use `geo_type='unknown'` and store the raw code as both `code` and `label`.
- `_resolve_metric` populates `self._metric_dims[metric_id]`. For unresolved metrics, `canonical_name` = the raw normalized label, `metric_family='unresolved'`.
- Expose `resolver.dim_caches()` returning all four dicts.

### 2. Document cache owned by `normalize_batch`

The resolver doesn't know documents. Track them at the batch level:

```python
# services/normalize/run.py
document_cache: dict[str, DocumentDimEntry] = {}

for table_info in tables:
    doc_id = table_info['document_id']
    if doc_id not in document_cache:
        # Pull from already-loaded table_info (joined raw.documents earlier)
        document_cache[doc_id] = DocumentDimEntry.from_row(table_info)
    ...
```

The normalize batch query already joins `raw.documents`; ensure it projects
title / source_url / document_type / language / published_date.

### 3. End-of-batch dim flush

```python
# services/normalize/dim_writer.py  (new)
def flush_dims(
    bq: BigQueryClient,
    project: str,
    cur_dataset: str,
    resolver: MappingResolver,
    document_cache: dict,
) -> dict:
    metric_dims = resolver.metric_dims()   # dict[metric_id, DimEntry]
    time_dims = resolver.time_dims()
    geo_dims = resolver.geo_dims()

    stats = {"metrics": 0, "times": 0, "geographies": 0, "documents": 0}
    stats["metrics"] = _merge_dim(
        bq, project, cur_dataset, "dim_metric",
        key="metric_id",
        rows=[_metric_row(e) for e in metric_dims.values()],
    )
    stats["times"] = _merge_dim(...)
    stats["geographies"] = _merge_dim(...)
    stats["documents"] = _merge_dim(
        bq, project, cur_dataset, "dim_document",
        key="document_id",
        rows=[d.to_bq_row() for d in document_cache.values()],
    )
    return stats


def _merge_dim(bq, project, dataset, table, key, rows) -> int:
    """Upload rows to a temp staging table, then MERGE into the real dim."""
    if not rows:
        return 0
    staging_table = f"_stage_{table}_{uuid4().hex[:8]}"
    bq.load_json_into(dataset, staging_table, rows, autodetect=True)

    merge_sql = f'''
        MERGE `{project}.{dataset}.{table}` T
        USING `{project}.{dataset}.{staging_table}` S
        ON T.{key} = S.{key}
        WHEN NOT MATCHED THEN INSERT ROW
        WHEN MATCHED THEN UPDATE SET
            {', '.join(f"{c} = COALESCE(S.{c}, T.{c})"
                       for c in _non_key_cols(table))}
    '''
    bq.execute_ddl(merge_sql)
    bq.execute_ddl(f"DROP TABLE `{project}.{dataset}.{staging_table}`")
    return len(rows)
```

MERGE semantics:

- **INSERT** unseen IDs with their resolved labels.
- **UPDATE** existing rows only when the incoming row has a non-null value
  for a column that's currently null. This preserves high-quality seeded
  entries (e.g. "Real GDP Growth" with its metric_family) while filling in
  holes.

### 4. Wire it into `normalize_batch`

```python
# services/normalize/run.py
stats = {...}
for idx, table_info in enumerate(tables, 1):
    ...

# At end of batch, flush dims
if stats["tables_processed"] > 0:
    dim_stats = flush_dims(bq_client, project_id, cur_dataset, resolver, document_cache)
    logger.info("Dim flush complete: %s", dim_stats)
    stats.update({f"dim_{k}": v for k, v in dim_stats.items()})

return stats
```

### 5. StatCan census geographies (DAUIDs, HRUIDs)

For StatCan census tables, the transform produces geography_ids from values
like `"1001001"` (a DAUID) or `"Eastern Health"` (an HRUID name). These don't
match `geography_dictionary.yaml`.

Handle inside `_resolve_geography`:

```python
def _resolve_geography(self, normalized, raw_value=None):
    ...
    # Existing matching logic
    if matched:
        return ResolvedMapping(geography_id=...)

    # Unresolved: still create a dim entry so the geo is queryable
    geo_id = _make_id("geo", normalized)
    geo_type = _infer_geo_type(normalized, raw_value)  # 'dissemination_area', 'health_region', etc.
    self._geo_dims[geo_id] = DimEntry(
        entity_id=geo_id,
        label=raw_value or normalized,
        geo_type=geo_type,
        code=raw_value,
    )
    return ResolvedMapping(geography_id=geo_id)
```

Where `_infer_geo_type`:
- 7-digit numeric → `dissemination_area`
- 5-digit numeric → `census_subdivision`
- Contains "Health" or "HR" → `health_region`
- Otherwise → `unknown`

Optional: expand `geography_dictionary.yaml` with aggregates (all 13 provinces,
all territories, Canada total) so those always resolve to friendly names even
when appearing by code.

### 6. Schema changes

None required — existing dim schemas accommodate this.

Optional additions to `dim_metric` for better agent UX:
- `last_seen_at TIMESTAMP` — helps detect stale metrics
- `sample_observation_id STRING` — an example fact row, useful for UI previews
- `inferred BOOL` — true if created via fallback (not from `metric_dictionary.yaml`)

### 7. Migration / backfill

One-time run after deploying the new resolver:

```bash
# Wipe existing dim tables (use TRUNCATE so streaming buffer doesn't block)
bq query --use_legacy_sql=false '
  TRUNCATE TABLE `duwillagence.cur.dim_metric`;
  TRUNCATE TABLE `duwillagence.cur.dim_time`;
  TRUNCATE TABLE `duwillagence.cur.dim_geography`;
  TRUNCATE TABLE `duwillagence.cur.dim_document`;
'

# Re-normalize — now flushes dim tables on completion
uv run python scripts/reprocess_normalize.py --all
```

The seed entries from `metric_dictionary.yaml` get re-created automatically by
the resolver's first pass through matching synonyms.

### 8. Tests

New unit tests:

- `tests/services/normalize/test_dim_flush.py`
  - `test_unresolved_metric_gets_dim_entry` — assert that a metric_id for an
    unmapped header has a `dim_metric` row after flush.
  - `test_seeded_metric_preserves_family` — when a raw header matches a YAML
    synonym, the flushed row keeps the synonym's `metric_family`, not
    'unresolved'.
  - `test_dauid_geography_classified_correctly` — `_infer_geo_type('1001001')`
    returns `'dissemination_area'`.

New integration test:

- `tests/integration/test_normalize_end_to_end.py` (or in existing file)
  - After `normalize_batch` runs against a synthetic fixture, assert:
    ```
    COUNT(DISTINCT metric_id FROM fact_observation)
      == COUNT(*) FROM dim_metric WHERE metric_id IN (...)
    ```
    Same for time, geography, document.
  - This is the regression guard for the original bug.

### 9. Metrics / observability

Add to the normalize summary log:
```
Normalization complete: {
    'tables_processed': 2173,
    'observations_created': 708932,
    'dim_metrics': 1085,
    'dim_times': 31,
    'dim_geographies': 14231,
    'dim_documents': 75,
    'errors': 0,
}
```

A post-run assertion in CI:
```sql
-- CI smoke check: no orphan fact rows
SELECT COUNT(*) FROM cur.fact_observation f
LEFT JOIN cur.dim_metric dm USING (metric_id)
WHERE dm.metric_id IS NULL
-- Expected: 0
```

Same for time, document. Geography can be non-zero if NULL is allowed.

---

## Effort estimate

| Task | Effort |
| ---- | ------ |
| Extend resolver with DimEntry caches (steps 1, 2) | 2 h |
| Write `dim_writer.py` with MERGE logic (step 3) | 2 h |
| Wire into normalize_batch + document cache (step 4) | 1 h |
| Geography inference + DAUID/HRUID support (step 5) | 1.5 h |
| Unit + integration tests (step 8) | 2 h |
| Migration + backfill verification | 30 m |
| **Total** | **~9 h** |

Roughly one focused engineering day.

---

## Risks / open questions

1. **MERGE on streaming-buffer rows.** If any dim table has recent streaming
   inserts, MERGE into it will fail. Solution: the `_merge_dim` helper should
   use load jobs into the staging table, then MERGE — MERGE operates against
   the main table's committed rows, and since we're only inserting new dims
   (no streaming to dims), buffer issues shouldn't arise. Worth testing.

2. **Metric name quality for composite labels.** Finance dimensional-long
   creates composite labels like `"Federal | Goods | Standard"`. These become
   `canonical_name` — readable but not canonical. Consider a follow-up step
   where a periodic reviewer flattens or renames these. For now, set
   `metric_family='composite_dimensional'` so they're filterable.

3. **DAUID cardinality.** If every DAUID becomes a dim_geography row, the
   table could grow to ~60K rows (roughly the number of Canadian dissemination
   areas). Not a problem for BQ but clutters the "list_geographies" tool.
   Mitigation: filter the tool to `geo_type IN ('country', 'province',
   'territory', 'region')` by default.

4. **French metric synonyms.** Several YAML entries have French synonyms
   (`canonical_name_fr`). Ensure the resolver still matches those — they
   should continue to resolve to the same canonical `metric_id` regardless
   of which synonym form appears in the data.

---

## Acceptance criteria

- [ ] `COUNT(*) FROM cur.dim_metric` ≥ `COUNT(DISTINCT metric_id) FROM cur.fact_observation`
- [ ] Same for time, geography (allowing for nullable geography_id), document
- [ ] No orphan `fact_observation` rows (INNER JOIN against each dim returns same count as `fact_observation`)
- [ ] Running normalize twice on the same data is idempotent (no duplicates)
- [ ] Agent's `describe_coverage` tool returns non-empty results for all three
      departments without manual backfill
- [ ] `populate_dim_time.py` and the manual `dim_metric` / `dim_document`
      backfills can be deleted
