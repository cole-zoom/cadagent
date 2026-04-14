"""Shared fixtures for trace-ca test suite."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml


# ---------------------------------------------------------------------------
# Minimal YAML mapping dictionaries for classifier / resolver tests
# ---------------------------------------------------------------------------

MINIMAL_METRICS = {
    "metrics": {
        "gdp_real_growth": {
            "canonical_name_en": "Real GDP Growth",
            "canonical_name_fr": "Croissance du PIB réel",
            "metric_family": "macroeconomic",
            "default_unit": "percent",
            "is_additive": False,
            "synonyms": ["real gdp growth"],
        },
        "cpi_inflation": {
            "canonical_name_en": "CPI Inflation",
            "metric_family": "macroeconomic",
            "default_unit": "percent",
            "is_additive": False,
            "synonyms": ["cpi inflation"],
        },
        "unemployment_rate": {
            "canonical_name_en": "Unemployment Rate",
            "metric_family": "labour",
            "default_unit": "percent",
            "is_additive": False,
            "synonyms": ["unemp. rate", "unemployment rate"],
        },
    }
}

MINIMAL_GEOGRAPHY = {
    "provinces": {
        "ON": {"name_en": "Ontario", "name_fr": "Ontario", "geo_type": "province"},
        "QC": {"name_en": "Quebec", "name_fr": "Québec", "geo_type": "province"},
    },
    "aggregates": {
        "canada": {"name_en": "Canada", "geo_type": "country", "code": "CA"},
        "total canada": {"name_en": "Canada", "geo_type": "country", "code": "CA"},
    },
}

MINIMAL_SCENARIOS = {
    "scenarios": {
        "actual": {"synonyms": ["actual", "historical"]},
        "projection": {"synonyms": ["projection", "forecast"]},
    }
}

MINIMAL_JUNK = {
    "junk_patterns": [
        "^@id$",
        "^@urn$",
        "^@lang$",
        "^@color$",
        "^@fontname$",
        "^@bold$",
        "^$",
    ]
}

MINIMAL_ATTRIBUTES = {
    "gender": {
        "attribute_type": "Gender",
        "values": [
            {"normalized": "women", "en": "Women", "fr": "Femmes"},
            {"normalized": "men", "en": "Men", "fr": "Hommes"},
        ],
    }
}


@pytest.fixture
def tmp_mappings_dir(tmp_path):
    """Create a temp directory with minimal YAML mapping dictionaries."""
    (tmp_path / "metric_dictionary.yaml").write_text(yaml.dump(MINIMAL_METRICS))
    (tmp_path / "geography_dictionary.yaml").write_text(yaml.dump(MINIMAL_GEOGRAPHY))
    (tmp_path / "scenario_dictionary.yaml").write_text(yaml.dump(MINIMAL_SCENARIOS))
    (tmp_path / "junk_headers.yaml").write_text(yaml.dump(MINIMAL_JUNK))
    (tmp_path / "attribute_dictionary.yaml").write_text(yaml.dump(MINIMAL_ATTRIBUTES))
    return tmp_path


# ---------------------------------------------------------------------------
# Sample data bytes
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_csv_bytes():
    return b"Year,Real GDP Growth,CPI Inflation\n2023,1.5,3.2\n2024,2.0,2.8\n"


@pytest.fixture
def sample_sdmx_xml_bytes():
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<DataSet>
  <Series Geography="Canada" Estimate="Total">
    <Obs TIME_PERIOD="2023" OBS_VALUE="100.5"/>
    <Obs TIME_PERIOD="2024" OBS_VALUE="102.3"/>
  </Series>
  <Series Geography="Ontario" Estimate="Total">
    <Obs TIME_PERIOD="2023" OBS_VALUE="50.1"/>
  </Series>
</DataSet>"""


@pytest.fixture
def sample_html_bytes():
    return b"""<html><body>
<table>
  <tr><th>Province</th><th>Population</th></tr>
  <tr><td>Ontario</td><td>15000000</td></tr>
  <tr><td>Quebec</td><td>8700000</td></tr>
</table>
</body></html>"""


# ---------------------------------------------------------------------------
# Mock clients
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_bq_client():
    client = MagicMock()
    client.insert_rows.return_value = []
    client.query.return_value = []
    client.table_exists.return_value = True
    client.execute_ddl.return_value = None
    client.get_existing_source_urls.return_value = set()
    return client


@pytest.fixture
def mock_gcs_client():
    client = MagicMock()
    client.upload_raw_file.return_value = "gs://test-bucket/raw/goc/department=fin/year=2024/document_id=abc/test.csv"
    client.upload_processed_file.return_value = "gs://test-bucket/processed/test.json"
    client.file_exists.return_value = True
    client.download_file.return_value = b"test,data\n1,2\n"
    return client
