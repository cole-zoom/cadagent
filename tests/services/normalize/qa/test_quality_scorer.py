"""Tests for services/normalize/qa/quality_scorer.py."""

from shared.models.observation import FactObservation
from services.normalize.qa.quality_scorer import QualityResult, score_observation


def _make_obs(**overrides) -> FactObservation:
    """Create a FactObservation with sensible defaults, overridden as needed."""
    defaults = {
        "observation_id": "test-1",
        "department_id": "fin",
        "document_id": "doc-1",
        "table_id": "tab-1",
        "metric_id": "met-1",
        "time_id": "time-1",
        "value_numeric": 100.0,
        "unit_raw": "percent",
    }
    defaults.update(overrides)
    return FactObservation(**defaults)


class TestScoreObservation:
    def test_perfect_observation(self):
        """Observation with all fields set has no issues -> score 1.0."""
        obs = _make_obs()
        result = score_observation(obs, mapping_confidence=0.9)
        assert result.quality_confidence == 1.0
        assert result.issue_codes == []

    def test_missing_metric(self):
        obs = _make_obs(metric_id="")
        result = score_observation(obs)
        assert "MISSING_METRIC" in result.issue_codes
        # 1.0 - 0.3 (metric) - 0.05 (missing unit? no, unit_raw is set) = 0.7
        assert result.quality_confidence == 0.7

    def test_missing_time(self):
        obs = _make_obs(time_id="")
        result = score_observation(obs)
        assert "MISSING_TIME" in result.issue_codes
        assert result.quality_confidence == 0.8

    def test_low_confidence(self):
        obs = _make_obs()
        result = score_observation(obs, mapping_confidence=0.5)
        assert "LOW_METRIC_CONFIDENCE" in result.issue_codes
        assert result.quality_confidence == 0.8

    def test_missing_value(self):
        obs = _make_obs(value_numeric=None, value_text=None, unit_raw="percent")
        result = score_observation(obs)
        assert "MISSING_VALUE" in result.issue_codes
        # 1.0 - 0.3 (missing value) = 0.7
        # No MISSING_UNIT because value_numeric is None
        assert result.quality_confidence == 0.7

    def test_implausible_value(self):
        obs = _make_obs(value_numeric=1e16)
        result = score_observation(obs)
        assert "IMPLAUSIBLE_VALUE" in result.issue_codes

    def test_missing_unit(self):
        obs = _make_obs(unit_raw=None)
        result = score_observation(obs)
        assert "MISSING_UNIT" in result.issue_codes
        assert result.quality_confidence == 0.95

    def test_multiple_issues(self):
        obs = _make_obs(metric_id="", time_id="", value_numeric=None, value_text=None, unit_raw=None)
        result = score_observation(obs, mapping_confidence=0.3)
        assert "MISSING_METRIC" in result.issue_codes
        assert "MISSING_TIME" in result.issue_codes
        assert "LOW_METRIC_CONFIDENCE" in result.issue_codes
        assert "MISSING_VALUE" in result.issue_codes
        # No MISSING_UNIT because value_numeric is None
        assert "MISSING_UNIT" not in result.issue_codes

    def test_score_clamped_to_zero(self):
        """Stacking enough penalties should clamp to 0.0, not go negative."""
        obs = _make_obs(metric_id="", time_id="", value_numeric=None, value_text=None, unit_raw=None)
        result = score_observation(obs, mapping_confidence=0.1)
        # 1.0 - 0.3 (metric) - 0.2 (time) - 0.2 (confidence) - 0.3 (value) = 0.0
        assert result.quality_confidence == 0.0

    def test_quality_result_to_bq_row(self):
        qr = QualityResult(
            observation_id="obs-1",
            quality_confidence=0.85,
            issue_codes=["MISSING_UNIT"],
            issue_notes=["Unit info missing"],
        )
        row = qr.to_bq_row()
        assert row["observation_id"] == "obs-1"
        assert row["quality_confidence"] == 0.85
        assert row["issue_codes"] == "MISSING_UNIT"
        assert row["issue_notes"] == "Unit info missing"
        assert row["review_status"] == "unreviewed"
        assert row["reviewed_by"] is None
        assert row["reviewed_at"] is None

    def test_quality_result_to_bq_row_multiple_issues(self):
        qr = QualityResult(
            observation_id="obs-2",
            quality_confidence=0.5,
            issue_codes=["MISSING_METRIC", "MISSING_TIME"],
            issue_notes=["No metric ID assigned", "No time dimension assigned"],
        )
        row = qr.to_bq_row()
        assert row["issue_codes"] == "MISSING_METRIC,MISSING_TIME"
        assert row["issue_notes"] == "No metric ID assigned; No time dimension assigned"

    def test_observation_id_propagated(self):
        obs = _make_obs(observation_id="unique-obs-42")
        result = score_observation(obs)
        assert result.observation_id == "unique-obs-42"

    def test_implausible_negative_value(self):
        """Large negative values are also implausible."""
        obs = _make_obs(value_numeric=-2e15)
        result = score_observation(obs)
        assert "IMPLAUSIBLE_VALUE" in result.issue_codes

    def test_value_text_present_not_missing(self):
        """If value_text is set but value_numeric is None, no MISSING_VALUE."""
        obs = _make_obs(value_numeric=None, value_text="some text", unit_raw="text")
        result = score_observation(obs)
        assert "MISSING_VALUE" not in result.issue_codes
        # No MISSING_UNIT because value_numeric is None
        assert "MISSING_UNIT" not in result.issue_codes
