# Open Canada CKAN API Guide

The Government of Canada publishes open data through [open.canada.ca](https://open.canada.ca), which runs on **CKAN** -- an open-source data management platform. Everything you can do on the website (search, filter, download) you can also do programmatically through the API. No API key or authentication is needed for read-only access.

## Quick Reference

| What | Value |
|---|---|
| API Base URL | `https://open.canada.ca/data/en/api/3/action/` |
| Web Base URL | `https://open.canada.ca/data/en/dataset/{id}` |
| Auth Required | No (read-only is fully public) |
| Response Format | JSON |
| Rate Limiting | Be respectful -- no documented hard limit, but don't hammer it |

---

## Core Concepts

### Datasets (Packages)

A **dataset** is the fundamental unit. It represents a collection of related data, e.g. "Planned Spending by Program" or "Proactive Disclosure - Travel Expenses". In CKAN's internals, datasets are called "packages" (you'll see this in API endpoint names).

Each dataset has:
- **Metadata**: title, description, organization, license, keywords, dates, etc.
- **Resources**: the actual data files (CSV, XLSX, XML, PDF, etc.) or links to them

### Resources

A dataset can contain multiple **resources** -- different years of the same data, different formats (CSV vs XLSX), English vs French versions, etc. Each resource has a download URL and format metadata.

### Organizations

Datasets are owned by **organizations** (government departments). For example, `tbs-sct` is Treasury Board of Canada Secretariat, `statcan` is Statistics Canada.

---

## API Endpoints

All endpoints follow the pattern:

```
https://open.canada.ca/data/en/api/3/action/{action_name}
```

Every response has this structure:

```json
{
  "help": "...",
  "success": true,
  "result": { ... }
}
```

If `success` is `false`, check the `error` field.

### package_search -- Search Datasets

The most useful endpoint. Searches across all datasets using Solr query syntax.

```
GET /api/3/action/package_search
```

#### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `q` | string | `"*:*"` | Search query. Searches titles, descriptions, keywords, etc. |
| `fq` | string | | Filter query (Solr syntax). Narrow results without affecting relevance scoring. |
| `sort` | string | `"score desc, metadata_modified desc"` | Sort order. Comma-separated `field direction` pairs. |
| `rows` | int | 10 | Number of results per page. Max 1000. |
| `start` | int | 0 | Offset for pagination. |
| `facet` | bool | true | Enable faceted counts in response. |
| `facet.field` | JSON list | `[]` | Fields to facet on, e.g. `["organization", "res_format"]`. |
| `facet.limit` | int | 50 | Max facet values returned per field. |
| `include_private` | bool | false | Include private datasets (only your orgs). |

#### Example: Basic Search

```bash
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&rows=5"
```

#### Example: Search with Facets

```bash
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&rows=5&facet.field=[\"organization\",\"res_format\"]&facet.limit=5"
```

Response includes `search_facets` showing top organizations and file formats across all results.

#### Example: Filter by Organization

```bash
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&fq=organization:tbs-sct&rows=10"
```

Only returns datasets owned by Treasury Board of Canada Secretariat.

#### Example: Sort by Most Recently Modified

```bash
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&sort=metadata_modified+desc&rows=10"
```

#### Example: Pagination

```bash
# Page 1 (results 1-100)
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&rows=100&start=0"

# Page 2 (results 101-200)
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&rows=100&start=100"

# Page 3 (results 201-300)
curl "https://open.canada.ca/data/en/api/3/action/package_search?q=spending&rows=100&start=200"
```

The `result.count` field tells you the total, so you know when to stop.

#### The `fq` Filter Query

`fq` uses Solr syntax and is powerful for narrowing results. Unlike `q`, it doesn't affect relevance scoring.

```bash
# Datasets from Statistics Canada
fq=organization:statcan

# Only datasets with CSV resources
fq=res_format:CSV

# Combined filters
fq=organization:tbs-sct+res_format:CSV

# Federal jurisdiction only
fq=jurisdiction:federal
```

### package_show -- Get a Single Dataset

Fetch full metadata and resources for a specific dataset by ID.

```
GET /api/3/action/package_show?id={dataset_id}
```

#### Example

```bash
curl "https://open.canada.ca/data/en/api/3/action/package_show?id=4535f6d2-eb2a-4339-a4bb-aaa2f0965da3"
```

Returns the same dataset structure you'd get from `package_search`, but for one specific dataset.

### organization_list -- List All Organizations

```bash
curl "https://open.canada.ca/data/en/api/3/action/organization_list"
```

Returns a flat array of organization slugs (e.g. `["tbs-sct", "statcan", "fin", ...]`).

Add `?all_fields=true` to get full details including title and description.

### organization_show -- Organization Details

```bash
curl "https://open.canada.ca/data/en/api/3/action/organization_show?id=tbs-sct"
```

Returns organization metadata including member count and package count.

### status_show -- Site Status

```bash
curl "https://open.canada.ca/data/en/api/3/action/status_show"
```

Returns site configuration, extensions list, CKAN version, etc.

---

## Response Structure: Dataset Object

When you get a dataset from `package_search` or `package_show`, here are the most useful fields:

```json
{
  "id": "4535f6d2-eb2a-4339-a4bb-aaa2f0965da3",
  "title": "Planned Spending by Program",
  "title_translated": { "en": "...", "fr": "..." },
  "notes": "Description of the dataset...",
  "notes_translated": { "en": "...", "fr": "..." },
  "organization": {
    "id": "...",
    "name": "tbs-sct",
    "title": "Treasury Board of Canada Secretariat | Secrétariat du Conseil du Trésor du Canada"
  },
  "date_published": "2017-04-28 00:00:00",
  "metadata_modified": "2026-03-28T13:51:35.358000",
  "metadata_created": "2017-06-01T...",
  "license_title": "Open Government Licence - Canada",
  "license_url": "https://open.canada.ca/en/open-government-licence-canada",
  "frequency": "P1Y",
  "keywords": { "en": ["spending", "budget", ...], "fr": [...] },
  "subject": ["economics_and_industry"],
  "num_resources": 12,
  "resources": [ ... ]
}
```

### Bilingual Fields

Many fields come as `{"en": "...", "fr": "..."}` dicts. Use the `_translated` variant when available (e.g. `title_translated` instead of `title`).

### Resource Object

Each resource in the `resources` array looks like:

```json
{
  "id": "eb06c087-...",
  "name": "Planned Spending by Program, 2014-17",
  "name_translated": { "en": "...", "fr": "..." },
  "format": "XLS",
  "url": "https://open.canada.ca/data/dataset/.../download/file.xls",
  "language": ["en"],
  "size": 84115,
  "last_modified": "2024-11-13T15:18:51.320921",
  "resource_type": "dataset"
}
```

**Important**: Some resource URLs are relative (start with `/data/...`). Prefix them with `https://open.canada.ca` to get the full download URL.

---

## URL Patterns

| What | Pattern |
|---|---|
| API action | `https://open.canada.ca/data/en/api/3/action/{action}` |
| Dataset web page | `https://open.canada.ca/data/en/dataset/{id}` |
| Resource download | Check `resources[].url` (may be absolute or relative) |
| Organization web page | `https://open.canada.ca/data/en/organization/{slug}` |

Switch `en` to `fr` in any URL for the French version.

---

## Python Quick Start

```python
import requests

API = "https://open.canada.ca/data/en/api/3/action"

# Search for spending datasets
resp = requests.get(f"{API}/package_search", params={"q": "spending", "rows": 5})
data = resp.json()

print(f"Total results: {data['result']['count']}")

for ds in data["result"]["results"]:
    print(f"  {ds['title']}")
    for res in ds.get("resources", []):
        print(f"    [{res['format']}] {res['url']}")
```

```python
# Get a specific dataset by ID
resp = requests.get(f"{API}/package_show", params={"id": "4535f6d2-eb2a-4339-a4bb-aaa2f0965da3"})
dataset = resp.json()["result"]
print(dataset["title"], "-", len(dataset["resources"]), "resources")
```

```python
# Paginate through ALL results
all_datasets = []
start = 0
while True:
    resp = requests.get(f"{API}/package_search", params={"q": "spending", "rows": 100, "start": start})
    result = resp.json()["result"]
    all_datasets.extend(result["results"])
    if start + 100 >= result["count"]:
        break
    start += 100

print(f"Fetched all {len(all_datasets)} datasets")
```

---

## Common Gotchas

1. **`rows` max is 1000** -- You can't get more than 1000 results per request. Use `start` to paginate.

2. **Relative resource URLs** -- Some URLs start with `/data/...`. Always check and prepend `https://open.canada.ca` if needed.

3. **Bilingual everything** -- Titles, descriptions, keywords all come in `en`/`fr`. Use the `_translated` fields.

4. **`name` vs `title`** -- On Open Canada, dataset `name` is usually the UUID (same as `id`). Use `title` for the human-readable name.

5. **Solr query syntax** -- The `q` parameter supports Solr/Lucene syntax: `q=title:spending`, `q=spending AND budget`, `q="exact phrase"`.

6. **No write access** -- The public API is read-only. Creating/editing datasets requires authentication and is not available to the public.

7. **The `facets` field is deprecated** -- Use `search_facets` instead. The old `facets` field returns an empty dict.

---

## Useful Search Queries

```bash
# All proactive disclosure datasets
q=proactive+disclosure

# Travel expenses specifically
q="travel expenses"

# Contracts over a certain value
q=contracts+spending

# Datasets from a specific department with CSV files
q=spending&fq=organization:tbs-sct+res_format:CSV

# Most recently updated datasets
q=spending&sort=metadata_modified+desc

# Datasets published in 2024
q=spending&fq=date_published:[2024-01-01+TO+2024-12-31]
```
