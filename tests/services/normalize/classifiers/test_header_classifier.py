"""Tests for services/normalize/classifiers/header_classifier.py."""

from services.normalize.classifiers.header_classifier import HeaderClassifier


class TestHeaderClassifier:
    def test_classify_empty(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("")
        assert result.header_class == "junk"
        assert result.confidence == 1.0
        assert result.method == "empty"

    def test_classify_junk_pattern_at_id(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("@id")
        assert result.header_class == "junk"
        assert result.confidence == 1.0
        assert result.method == "junk_pattern"

    def test_classify_junk_pattern_at_color(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        # normalize_header lowercases, junk patterns compiled with IGNORECASE
        result = clf.classify("@Color")
        assert result.header_class == "junk"
        assert result.confidence == 1.0
        assert result.method == "junk_pattern"

    def test_classify_statcan_value_column(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("@OBS_VALUE")
        assert result.header_class == "metric"
        assert result.confidence == 0.9
        assert result.method == "statcan_value_column"

    def test_classify_statcan_unit_column(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("@UOM")
        assert result.header_class == "unit"
        assert result.confidence == 0.95
        assert result.method == "statcan_unit_column"

    def test_classify_time_fiscal_year(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("2023-24")
        assert result.header_class == "time"
        assert result.confidence == 0.95
        assert result.method == "time_parser"

    def test_classify_time_calendar_year(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("2023")
        assert result.header_class == "time"
        assert result.confidence == 0.95
        assert result.method == "time_parser"

    def test_classify_geography_province(self, tmp_mappings_dir):
        """ON -> normalized to 'on' which is in geo_lookup (key 'ON' lowered to 'on')."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("ON")
        assert result.header_class == "geography"
        assert result.confidence == 0.95

    def test_classify_geography_canada(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("Canada")
        assert result.header_class == "geography"
        assert result.confidence == 0.95

    def test_classify_geography_statcan(self, tmp_mappings_dir):
        """@Geography is a special case: normalized to '@geography'."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("@Geography")
        assert result.header_class == "geography"
        assert result.confidence == 0.95
        assert result.method == "statcan_geo_attribute"

    def test_classify_scenario(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("Projection")
        assert result.header_class == "scenario"
        assert result.confidence == 0.95
        assert result.method == "scenario_dictionary"
        assert result.canonical_hint == "projection"

    def test_classify_statcan_attribute(self, tmp_mappings_dir):
        """@Gender -> normalized '@gender', starts with '@', not junk, not geo -> attribute."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("@Gender")
        assert result.header_class == "attribute"
        assert result.confidence == 0.8
        assert result.method == "statcan_dimension_attribute"

    def test_classify_metric_known(self, tmp_mappings_dir):
        """'Real GDP Growth' -> normalized 'real gdp growth' which is in metric synonyms."""
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("Real GDP Growth")
        assert result.header_class == "metric"
        assert result.confidence == 0.9
        assert result.method == "metric_dictionary"
        assert result.canonical_hint == "gdp_real_growth"

    def test_classify_unit(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("per cent")
        assert result.header_class == "unit"
        assert result.confidence == 0.9
        assert result.method == "unit_pattern"

    def test_classify_fallback(self, tmp_mappings_dir):
        clf = HeaderClassifier(mappings_dir=tmp_mappings_dir)
        result = clf.classify("Some Unknown Header")
        assert result.header_class == "metric"
        assert result.confidence == 0.3
        assert result.method == "fallback"
