"""Seed BigQuery dimension tables from YAML mapping dictionaries.

Pre-populates: dim_department, dim_geography, dim_scenario,
initial dim_metric, dim_attribute_type, dim_attribute_value.

Usage:
    python scripts/seed_mappings.py
"""

import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.clients.bigquery import BigQueryClient
from shared.config.logging import configure_logging
from shared.config.settings import settings

MAPPINGS_DIR = Path(__file__).resolve().parent.parent / "mappings"


def _make_id(prefix: str, value: str | bool) -> str:
    value = str(value).lower().strip()
    return hashlib.sha256(f"{prefix}|{value}".encode()).hexdigest()[:24]


def seed_departments(bq: BigQueryClient, dataset: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {"department_id": "fin", "department_code": "fin", "department_name_en": "Finance Canada",
         "department_name_fr": "Finances Canada", "active_flag": True, "created_at": now},
        {"department_id": "statcan", "department_code": "statcan", "department_name_en": "Statistics Canada",
         "department_name_fr": "Statistique Canada", "active_flag": True, "created_at": now},
        {"department_id": "tbs-sct", "department_code": "tbs-sct",
         "department_name_en": "Treasury Board of Canada Secretariat",
         "department_name_fr": "Secrétariat du Conseil du Trésor du Canada",
         "active_flag": True, "created_at": now},
    ]
    bq.insert_rows(dataset, "dim_department", rows)
    print(f"Seeded {len(rows)} departments")


def seed_geography(bq: BigQueryClient, dataset: str) -> None:
    path = MAPPINGS_DIR / "geography_dictionary.yaml"
    if not path.exists():
        print("geography_dictionary.yaml not found, skipping")
        return

    data = yaml.safe_load(path.read_text())
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    # The YAML has a top-level "geographies" dict keyed by geo code (CA, ON, US, etc.)
    geos = data.get("geographies", data)
    for key, val in geos.items():
        if not isinstance(val, dict):
            continue
        rows.append({
            "geography_id": _make_id("geo", key),
            "geo_type": val.get("geo_level", val.get("geo_type", "unknown")),
            "code": val.get("province_code", val.get("iso_alpha2", key)),
            "name_en": val.get("canonical_name_en", val.get("name_en", key)),
            "name_fr": val.get("canonical_name_fr", val.get("name_fr", "")),
            "created_at": now,
        })

    bq.insert_rows(dataset, "dim_geography", rows)
    print(f"Seeded {len(rows)} geographies")


def seed_scenarios(bq: BigQueryClient, dataset: str) -> None:
    path = MAPPINGS_DIR / "scenario_dictionary.yaml"
    if not path.exists():
        print("scenario_dictionary.yaml not found, skipping")
        return

    data = yaml.safe_load(path.read_text())
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for scenario_id, info in data.get("scenarios", {}).items():
        rows.append({
            "scenario_id": _make_id("scenario", scenario_id),
            "scenario_name": scenario_id,
            "scenario_group": info.get("group"),
            "created_at": now,
        })

    bq.insert_rows(dataset, "dim_scenario", rows)
    print(f"Seeded {len(rows)} scenarios")


def seed_metrics(bq: BigQueryClient, dataset: str) -> None:
    path = MAPPINGS_DIR / "metric_dictionary.yaml"
    if not path.exists():
        print("metric_dictionary.yaml not found, skipping")
        return

    data = yaml.safe_load(path.read_text())
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for metric_id, info in data.get("metrics", {}).items():
        rows.append({
            "metric_id": _make_id("metric", metric_id),
            "canonical_name": info.get("canonical_name_en", metric_id),
            "canonical_name_fr": info.get("canonical_name_fr"),
            "metric_family": info.get("metric_family"),
            "default_unit_id": info.get("default_unit"),
            "description": info.get("description"),
            "is_additive": info.get("is_additive", False),
            "created_at": now,
        })

    bq.insert_rows(dataset, "dim_metric", rows)
    print(f"Seeded {len(rows)} metrics")


def seed_attributes(bq: BigQueryClient, dataset: str) -> None:
    path = MAPPINGS_DIR / "attribute_dictionary.yaml"
    if not path.exists():
        print("attribute_dictionary.yaml not found, skipping")
        return

    data = yaml.safe_load(path.read_text())
    now = datetime.now(timezone.utc).isoformat()
    type_rows = []
    value_rows = []

    attrs = data.get("attributes", data)
    for attr_type_key, info in attrs.items():
        if not isinstance(info, dict):
            continue
        attr_type_id = _make_id("attr_type", attr_type_key)
        type_rows.append({
            "attribute_type_id": attr_type_id,
            "attribute_type_name": info.get("canonical_name_en", info.get("attribute_type", attr_type_key)),
            "description": info.get("description"),
            "created_at": now,
        })

        values = info.get("values", {})
        # values can be a dict (keyed by value name) or a list
        if isinstance(values, dict):
            for val_key, val_info in values.items():
                if not isinstance(val_info, dict):
                    continue
                value_rows.append({
                    "attribute_value_id": _make_id("attr_val", f"{attr_type_key}|{val_key}"),
                    "attribute_type_id": attr_type_id,
                    "value_en": val_info.get("canonical_name_en", val_key),
                    "value_fr": val_info.get("canonical_name_fr", ""),
                    "normalized_value": val_key,
                    "created_at": now,
                })
        elif isinstance(values, list):
            for val in values:
                value_rows.append({
                    "attribute_value_id": _make_id("attr_val", f"{attr_type_key}|{val.get('normalized', '')}"),
                    "attribute_type_id": attr_type_id,
                    "value_en": val.get("en", val.get("canonical_name_en", "")),
                    "value_fr": val.get("fr", val.get("canonical_name_fr", "")),
                    "normalized_value": val.get("normalized", ""),
                    "created_at": now,
                })

    bq.insert_rows(dataset, "dim_attribute_type", type_rows)
    bq.insert_rows(dataset, "dim_attribute_value", value_rows)
    print(f"Seeded {len(type_rows)} attribute types and {len(value_rows)} attribute values")


def main() -> None:
    configure_logging(settings.log_level, service="seed")
    bq = BigQueryClient(project_id=settings.gcp_project_id)
    dataset = settings.bq_cur_dataset

    seed_departments(bq, dataset)
    seed_geography(bq, dataset)
    seed_scenarios(bq, dataset)
    seed_metrics(bq, dataset)
    seed_attributes(bq, dataset)
    print("Seeding complete.")


if __name__ == "__main__":
    main()
