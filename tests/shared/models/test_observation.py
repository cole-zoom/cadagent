"""Tests for shared/models/observation.py."""

import pytest

from shared.models.observation import FactObservation


class TestFactObservation:
    def test_required_fields(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        assert obs.observation_id == "obs1"
        assert obs.department_id == "dept1"
        assert obs.document_id == "d1"
        assert obs.table_id == "t1"
        assert obs.metric_id == "m1"
        assert obs.time_id == "time1"

    def test_nullable_fields_default_none(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        assert obs.geography_id is None
        assert obs.scenario_id is None
        assert obs.value_numeric is None
        assert obs.value_text is None
        assert obs.unit_raw is None
        assert obs.currency_code is None
        assert obs.source_row_number is None
        assert obs.source_column_number is None
        assert obs.quality_score is None

    def test_default_scale_factor(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        assert obs.scale_factor == 1.0

    def test_created_at_auto_set(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        assert obs.created_at is not None
        assert len(obs.created_at) > 0

    def test_to_bq_row_has_all_fields(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        row = obs.to_bq_row()
        expected_keys = {
            "observation_id", "department_id", "document_id", "table_id",
            "metric_id", "time_id", "geography_id", "scenario_id",
            "value_numeric", "value_text", "unit_raw", "scale_factor",
            "currency_code", "source_row_number", "source_column_number",
            "quality_score", "created_at",
        }
        assert set(row.keys()) == expected_keys

    def test_to_bq_row_values(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
            geography_id="geo1",
            scenario_id="scen1",
            value_numeric=42.5,
            scale_factor=1000.0,
        )
        row = obs.to_bq_row()
        assert row["observation_id"] == "obs1"
        assert row["geography_id"] == "geo1"
        assert row["scenario_id"] == "scen1"
        assert row["value_numeric"] == 42.5
        assert row["scale_factor"] == 1000.0

    def test_to_bq_row_returns_dict(self):
        obs = FactObservation(
            observation_id="obs1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            metric_id="m1",
            time_id="time1",
        )
        row = obs.to_bq_row()
        assert isinstance(row, dict)
