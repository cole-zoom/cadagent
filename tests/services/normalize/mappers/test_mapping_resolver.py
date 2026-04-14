"""Tests for services/normalize/mappers/mapping_resolver.py."""

from services.normalize.mappers.mapping_resolver import MappingResolver, _make_id


class TestMakeId:
    def test_deterministic(self):
        """Calling _make_id with the same inputs twice gives the same result."""
        id1 = _make_id("test", "value")
        id2 = _make_id("test", "value")
        assert id1 == id2

    def test_length_is_24(self):
        result = _make_id("prefix", "some_value")
        assert len(result) == 24

    def test_hex_string(self):
        result = _make_id("metric", "gdp")
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_prefix_different_id(self):
        id1 = _make_id("time", "2023")
        id2 = _make_id("geo", "2023")
        assert id1 != id2

    def test_different_value_different_id(self):
        id1 = _make_id("metric", "gdp")
        id2 = _make_id("metric", "cpi")
        assert id1 != id2


class TestMappingResolver:
    def test_resolve_junk(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("", "junk", None)
        assert result.is_junk is True

    def test_resolve_time(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("2023-24", "time", None)
        assert result.time_id is not None
        assert len(result.time_id) == 24

    def test_resolve_time_with_scenario(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("Projection 2024-2025", "time", "projection")
        assert result.time_id is not None
        assert result.scenario_id is not None
        assert len(result.time_id) == 24
        assert len(result.scenario_id) == 24

    def test_resolve_geography(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("ON", "geography", None)
        assert result.geography_id is not None
        assert len(result.geography_id) == 24

    def test_resolve_scenario(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("actual", "scenario", "actual")
        assert result.scenario_id is not None
        assert len(result.scenario_id) == 24

    def test_resolve_metric_known(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("Real GDP Growth", "metric", None)
        assert result.metric_id is not None
        assert len(result.metric_id) == 24

    def test_resolve_metric_unknown(self, tmp_mappings_dir):
        """Unknown metrics still get a fallback-generated ID."""
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("Unknown Metric", "metric", None)
        assert result.metric_id is not None
        assert len(result.metric_id) == 24

    def test_resolve_attribute(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("@Gender", "attribute", None)
        assert result.attribute_type_id is not None
        assert len(result.attribute_type_id) == 24

    def test_resolve_unit_returns_empty_mapping(self, tmp_mappings_dir):
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        result = resolver.resolve("per cent", "unit", None)
        assert result.is_junk is False
        assert result.metric_id is None
        assert result.time_id is None

    def test_resolve_geography_caches(self, tmp_mappings_dir):
        """Resolving the same geography twice should give the same ID (cached)."""
        resolver = MappingResolver(mappings_dir=tmp_mappings_dir)
        r1 = resolver.resolve("ON", "geography", None)
        r2 = resolver.resolve("ON", "geography", None)
        assert r1.geography_id == r2.geography_id
