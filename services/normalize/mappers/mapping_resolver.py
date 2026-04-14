"""Resolve classified headers to canonical entity IDs.

For each classified header, look up or create the corresponding
dimension entry (time, geography, scenario, metric, attribute)
and return the canonical ID.
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from shared.utils.text_normalization import normalize_header
from shared.utils.time_parsing import parse_time

logger = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "mappings"


@dataclass
class MappingCandidate:
    header_id: str
    canonical_entity_type: str  # metric, time, geography, scenario, attribute_type, attribute_value
    canonical_entity_id: str | None
    candidate_method: str
    candidate_score: float
    approved_flag: bool | None = None


@dataclass
class ResolvedMapping:
    metric_id: str | None = None
    time_id: str | None = None
    geography_id: str | None = None
    scenario_id: str | None = None
    attribute_type_id: str | None = None
    attribute_value_id: str | None = None
    is_value_column: bool = False
    is_junk: bool = False


class MappingResolver:
    def __init__(self, mappings_dir: Path | None = None):
        self.mappings_dir = mappings_dir or MAPPINGS_DIR
        self._metrics: dict = {}
        self._geography: dict = {}
        self._scenarios: dict = {}
        self._loaded = False

        # Caches for dimension IDs we've already resolved
        self._time_cache: dict[str, str] = {}
        self._geo_cache: dict[str, str] = {}
        self._scenario_cache: dict[str, str] = {}
        self._metric_cache: dict[str, str] = {}

    def _load(self) -> None:
        if self._loaded:
            return
        self._load_metrics()
        self._load_geography()
        self._load_scenarios()
        self._loaded = True

    def _load_metrics(self) -> None:
        path = self.mappings_dir / "metric_dictionary.yaml"
        if path.exists():
            self._metrics = yaml.safe_load(path.read_text()).get("metrics", {})

    def _load_geography(self) -> None:
        path = self.mappings_dir / "geography_dictionary.yaml"
        if path.exists():
            self._geography = yaml.safe_load(path.read_text())

    def _load_scenarios(self) -> None:
        path = self.mappings_dir / "scenario_dictionary.yaml"
        if path.exists():
            self._scenarios = yaml.safe_load(path.read_text()).get("scenarios", {})

    def resolve(
        self, header_raw: str, header_class: str, classification_hint: str | None = None
    ) -> ResolvedMapping:
        """Resolve a classified header to canonical entity IDs."""
        self._load()
        normalized = normalize_header(header_raw)

        if header_class == "junk":
            return ResolvedMapping(is_junk=True)

        if header_class in ("time", "time_range"):
            return self._resolve_time(header_raw, classification_hint)

        if header_class == "geography":
            return self._resolve_geography(normalized)

        if header_class == "scenario":
            return self._resolve_scenario(normalized, classification_hint)

        if header_class == "metric":
            return self._resolve_metric(normalized)

        if header_class == "attribute":
            return self._resolve_attribute(normalized, header_raw)

        if header_class == "unit":
            return ResolvedMapping()

        return ResolvedMapping()

    def _resolve_time(self, header_raw: str, scenario_hint: str | None) -> ResolvedMapping:
        parsed = parse_time(header_raw)
        if not parsed:
            return ResolvedMapping()

        time_id = _make_id("time", parsed.label)
        self._time_cache[parsed.label] = time_id

        mapping = ResolvedMapping(time_id=time_id)

        if parsed.scenario_hint or scenario_hint:
            hint = parsed.scenario_hint or scenario_hint
            scenario_id = _make_id("scenario", hint)
            mapping.scenario_id = scenario_id

        return mapping

    def _resolve_geography(self, normalized: str) -> ResolvedMapping:
        if normalized in self._geo_cache:
            return ResolvedMapping(geography_id=self._geo_cache[normalized])

        # Search all sections of the geography dictionary
        for section_data in self._geography.values():
            if isinstance(section_data, dict):
                for key, val in section_data.items():
                    if key.lower() == normalized or val.get("name_en", "").lower() == normalized:
                        geo_id = _make_id("geo", key)
                        self._geo_cache[normalized] = geo_id
                        return ResolvedMapping(geography_id=geo_id)

        # Fallback: generate an ID
        geo_id = _make_id("geo", normalized)
        self._geo_cache[normalized] = geo_id
        return ResolvedMapping(geography_id=geo_id)

    def _resolve_scenario(self, normalized: str, hint: str | None) -> ResolvedMapping:
        scenario_name = hint or normalized
        for scenario_id, info in self._scenarios.items():
            if scenario_name in [s.lower() for s in info.get("synonyms", [])]:
                sid = _make_id("scenario", scenario_id)
                return ResolvedMapping(scenario_id=sid)

        sid = _make_id("scenario", scenario_name)
        return ResolvedMapping(scenario_id=sid)

    def _resolve_metric(self, normalized: str) -> ResolvedMapping:
        if normalized in self._metric_cache:
            return ResolvedMapping(metric_id=self._metric_cache[normalized])

        for metric_id, info in self._metrics.items():
            synonyms = [s.lower() for s in info.get("synonyms", [])]
            if normalized in synonyms:
                mid = _make_id("metric", metric_id)
                self._metric_cache[normalized] = mid
                return ResolvedMapping(metric_id=mid)

        # Unresolved -- generate a tentative ID
        mid = _make_id("metric", normalized)
        self._metric_cache[normalized] = mid
        return ResolvedMapping(metric_id=mid)

    def _resolve_attribute(self, normalized: str, header_raw: str) -> ResolvedMapping:
        # StatCan @DimensionName -> attribute_type is the dimension name
        if normalized.startswith("@"):
            attr_type_name = normalized[1:].replace("_", " ")
        else:
            attr_type_name = normalized

        attr_type_id = _make_id("attr_type", attr_type_name)
        return ResolvedMapping(attribute_type_id=attr_type_id)


def _make_id(prefix: str, value: str) -> str:
    """Generate a deterministic ID for a canonical entity."""
    return hashlib.sha256(f"{prefix}|{value.lower().strip()}".encode()).hexdigest()[:24]
