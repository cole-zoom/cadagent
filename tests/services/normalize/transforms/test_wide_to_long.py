"""Tests for services/normalize/transforms/wide_to_long.py."""

from services.normalize.classifiers.header_classifier import HeaderClassifier
from services.normalize.mappers.mapping_resolver import MappingResolver
from services.normalize.transforms.wide_to_long import (
    FinanceTransformStrategy,
    StatcanTransformStrategy,
    TbsSctTransformStrategy,
    _find_col,
    _try_numeric,
    get_strategy,
)


class TestGetStrategy:
    def test_fin(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = get_strategy("fin", clf, res)
        assert isinstance(strategy, FinanceTransformStrategy)

    def test_statcan(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = get_strategy("statcan", clf, res)
        assert isinstance(strategy, StatcanTransformStrategy)

    def test_tbs_sct(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = get_strategy("tbs-sct", clf, res)
        assert isinstance(strategy, TbsSctTransformStrategy)

    def test_unknown_defaults_to_finance(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = get_strategy("unknown", clf, res)
        assert isinstance(strategy, FinanceTransformStrategy)


class TestFindCol:
    def test_found(self):
        assert _find_col(["a", "b", "amount"], ["amount", "total"]) == 2

    def test_not_found(self):
        assert _find_col(["a", "b"], ["x"]) is None

    def test_first_candidate_wins(self):
        assert _find_col(["total", "amount"], ["amount", "total"]) == 1

    def test_empty_headers(self):
        assert _find_col([], ["x"]) is None

    def test_empty_candidates(self):
        assert _find_col(["a", "b"], []) is None


class TestTryNumeric:
    def test_integer_string(self):
        assert _try_numeric("100") == 100.0

    def test_comma_separated(self):
        assert _try_numeric("1,234") == 1234.0

    def test_dash_returns_none(self):
        assert _try_numeric("-") is None

    def test_ellipsis_returns_none(self):
        assert _try_numeric("...") is None

    def test_none_returns_none(self):
        assert _try_numeric(None) is None

    def test_empty_returns_none(self):
        assert _try_numeric("") is None

    def test_none_string_returns_none(self):
        assert _try_numeric("None") is None

    def test_float_string(self):
        assert _try_numeric("3.14") == 3.14

    def test_dollar_sign_stripped(self):
        assert _try_numeric("$1,000") == 1000.0

    def test_percent_sign_stripped(self):
        assert _try_numeric("50%") == 50.0

    def test_na_returns_none(self):
        assert _try_numeric("n/a") is None

    def test_negative_number(self):
        assert _try_numeric("-42.5") == -42.5


class TestFinanceTimeAsColumns:
    def test_time_as_columns(self, tmp_mappings_dir):
        """Time headers detected, row labels become fallback metrics."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = FinanceTransformStrategy(clf, res)

        headers = ["", "2023-24", "2024-25"]
        rows = [
            ["Revenues", "100", "110"],
            ["Expenses", "90", "95"],
        ]

        observations = strategy.transform(headers, rows, "fin", "doc-1", "tab-1")

        # 2 rows x 2 time columns = 4 observations
        assert len(observations) == 4
        for obs in observations:
            assert obs.time_id != ""
            assert obs.metric_id is not None
            assert obs.department_id == "fin"
            assert obs.document_id == "doc-1"
            assert obs.table_id == "tab-1"


class TestFinanceGeoAsColumns:
    def test_geo_as_columns(self, tmp_mappings_dir):
        """Geography headers > time headers -> geo-as-columns strategy."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = FinanceTransformStrategy(clf, res)

        headers = ["Year", "ON", "QC", "Canada"]
        rows = [
            ["2023", "100", "200", "300"],
        ]

        observations = strategy.transform(headers, rows, "fin", "doc-1", "tab-1")

        # "Year" -> fallback metric, but first col is the row label
        # "2023" is classified as time from the row label
        # ON, QC, Canada are all geography -> 3 geo columns x 1 row = 3 observations
        assert len(observations) == 3
        for obs in observations:
            assert obs.geography_id is not None
            assert obs.time_id is not None
            assert obs.value_numeric is not None


class TestStatcanTransform:
    def test_sdmx_observations(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = StatcanTransformStrategy(clf, res)

        headers = ["@TIME_PERIOD", "@OBS_VALUE", "@Geography"]
        rows = [
            ["2023", "100.5", "Canada"],
            ["2024", "102.3", "Ontario"],
        ]

        observations = strategy.transform(headers, rows, "statcan", "doc-2", "tab-2")

        assert len(observations) == 2
        assert observations[0].value_numeric == 100.5
        assert observations[1].value_numeric == 102.3
        for obs in observations:
            assert obs.department_id == "statcan"
            assert obs.time_id != ""

    def test_missing_obs_value_column_returns_empty(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = StatcanTransformStrategy(clf, res)

        headers = ["@TIME_PERIOD", "@Geography"]
        rows = [["2023", "Canada"]]

        observations = strategy.transform(headers, rows, "statcan", "doc-2", "tab-2")
        assert len(observations) == 0


class TestTbsSctFinancial:
    def test_financial_transform(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = TbsSctTransformStrategy(clf, res)

        headers = ["Organization", "Vote", "Amount"]
        rows = [
            ["Dept A", "1", "1000000"],
            ["Dept B", "5", "2000000"],
        ]

        observations = strategy.transform(headers, rows, "tbs-sct", "doc-3", "tab-3")

        assert len(observations) == 2
        assert observations[0].value_numeric == 1000000.0
        assert observations[1].value_numeric == 2000000.0
        for obs in observations:
            assert obs.department_id == "tbs-sct"


class TestTbsSctSurvey:
    def test_survey_transform(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = TbsSctTransformStrategy(clf, res)

        headers = ["QUESTION", "SCORE100", "AGREE"]
        rows = [
            ["Q1", "75.5", "80.2"],
            ["Q2", "62.3", "55.1"],
        ]

        observations = strategy.transform(headers, rows, "tbs-sct", "doc-4", "tab-4")

        assert len(observations) == 2
        assert observations[0].value_numeric == 75.5
        assert observations[1].value_numeric == 62.3
        for obs in observations:
            assert obs.department_id == "tbs-sct"


class TestTbsSctGeneric:
    def test_generic_returns_empty(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        res = MappingResolver(mappings_dir=tmp_mappings_dir)
        strategy = TbsSctTransformStrategy(clf, res)

        headers = ["Name", "Description"]
        rows = [["Foo", "Bar"]]

        observations = strategy.transform(headers, rows, "tbs-sct", "doc-5", "tab-5")
        assert len(observations) == 0
