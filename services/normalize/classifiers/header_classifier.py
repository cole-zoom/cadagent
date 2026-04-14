"""Header classification engine.

Classifies each raw header into one of:
metric, time, time_range, geography, scenario, attribute, unit, junk.

Uses a layered approach in priority order:
1. Junk filter (regex patterns)
2. Time detection (deterministic parsers)
3. Geography detection (dictionary lookup)
4. Scenario detection (synonym match)
5. StatCan attribute detection (@Dimension pattern)
6. Known metric match (synonym lookup)
7. Unit detection
8. Fallback to metric with low confidence
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from shared.utils.text_normalization import normalize_header
from shared.utils.time_parsing import parse_time

logger = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "mappings"

# StatCan observation value columns -- these are the measure, not junk
STATCAN_VALUE_COLUMNS = {"@obs_value", "obs", "value"}

# StatCan unit/scale columns -- classified as unit
STATCAN_UNIT_COLUMNS = {"@uom", "uom", "uom_id", "@scalar_factor", "scalar_factor", "scalar_id"}


@dataclass
class ClassificationResult:
    header_class: str
    confidence: float
    method: str
    canonical_hint: str | None = None  # For scenario/geo, the resolved value


class HeaderClassifier:
    def __init__(self, mappings_dir: Path | None = None):
        self.mappings_dir = mappings_dir or MAPPINGS_DIR
        self._junk_patterns: list[re.Pattern] = []
        self._geo_lookup: dict[str, str] = {}  # normalized -> geo_type
        self._scenario_lookup: dict[str, str] = {}  # normalized -> scenario_name
        self._metric_synonyms: dict[str, str] = {}  # normalized -> metric_id
        self._attribute_types: set[str] = set()
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return

        self._load_junk_patterns()
        self._load_geography()
        self._load_scenarios()
        self._load_metrics()
        self._load_attributes()
        self._loaded = True

    def _load_junk_patterns(self) -> None:
        path = self.mappings_dir / "junk_headers.yaml"
        if path.exists():
            data = yaml.safe_load(path.read_text())
            for pattern in data.get("junk_patterns", []):
                self._junk_patterns.append(re.compile(pattern, re.IGNORECASE))

    def _load_geography(self) -> None:
        path = self.mappings_dir / "geography_dictionary.yaml"
        if not path.exists():
            return
        data = yaml.safe_load(path.read_text())
        for section in data.values():
            if isinstance(section, dict):
                for key, val in section.items():
                    normalized = key.lower().strip()
                    self._geo_lookup[normalized] = val.get("geo_type", "unknown")
                    # Also add the English name as a lookup
                    name_en = val.get("name_en", "").lower().strip()
                    if name_en:
                        self._geo_lookup[name_en] = val.get("geo_type", "unknown")

    def _load_scenarios(self) -> None:
        path = self.mappings_dir / "scenario_dictionary.yaml"
        if not path.exists():
            return
        data = yaml.safe_load(path.read_text())
        for scenario_id, info in data.get("scenarios", {}).items():
            for synonym in info.get("synonyms", []):
                self._scenario_lookup[synonym.lower().strip()] = scenario_id

    def _load_metrics(self) -> None:
        path = self.mappings_dir / "metric_dictionary.yaml"
        if not path.exists():
            return
        data = yaml.safe_load(path.read_text())
        for metric_id, info in data.get("metrics", {}).items():
            for synonym in info.get("synonyms", []):
                self._metric_synonyms[synonym.lower().strip()] = metric_id

    def _load_attributes(self) -> None:
        path = self.mappings_dir / "attribute_dictionary.yaml"
        if not path.exists():
            return
        data = yaml.safe_load(path.read_text())
        for attr_type in data:
            self._attribute_types.add(attr_type.lower())

    def classify(self, header_raw: str) -> ClassificationResult:
        """Classify a single raw header string."""
        self._load()

        normalized = normalize_header(header_raw)

        if not normalized:
            return ClassificationResult("junk", 1.0, "empty")

        # 1. Junk filter
        for pattern in self._junk_patterns:
            if pattern.match(normalized):
                return ClassificationResult("junk", 1.0, "junk_pattern")

        # StatCan value columns are the measure
        if normalized in STATCAN_VALUE_COLUMNS:
            return ClassificationResult("metric", 0.9, "statcan_value_column", "observation_value")

        # StatCan unit columns
        if normalized in STATCAN_UNIT_COLUMNS:
            return ClassificationResult("unit", 0.95, "statcan_unit_column")

        # 2. Time detection
        parsed_time = parse_time(header_raw)
        if parsed_time:
            time_class = "time_range" if parsed_time.time_type == "range" else "time"
            return ClassificationResult(
                time_class, 0.95, "time_parser",
                canonical_hint=parsed_time.scenario_hint,
            )

        # 3. Geography detection
        if normalized in self._geo_lookup:
            return ClassificationResult("geography", 0.95, "geo_dictionary")

        # StatCan @Geography
        if normalized == "@geography":
            return ClassificationResult("geography", 0.95, "statcan_geo_attribute")

        # 4. Scenario detection
        if normalized in self._scenario_lookup:
            return ClassificationResult(
                "scenario", 0.95, "scenario_dictionary",
                canonical_hint=self._scenario_lookup[normalized],
            )

        # 5. StatCan attribute detection (@DimensionName pattern)
        if normalized.startswith("@") and len(normalized) > 1:
            # Strip @ and check if it's a known junk field (already handled above)
            dimension_name = normalized[1:].replace("_", " ").lower()
            return ClassificationResult("attribute", 0.8, "statcan_dimension_attribute")

        # 6. Known metric match
        if normalized in self._metric_synonyms:
            return ClassificationResult(
                "metric", 0.9, "metric_dictionary",
                canonical_hint=self._metric_synonyms[normalized],
            )

        # 7. Unit detection
        unit_patterns = [r"^per cent$", r"^percent$", r"^%$", r"^\$$", r"^millions? of dollars$",
                         r"^billions?$", r"^thousands?$", r"^units?$"]
        for pattern in unit_patterns:
            if re.match(pattern, normalized):
                return ClassificationResult("unit", 0.9, "unit_pattern")

        # 8. Fallback: assume metric with low confidence
        return ClassificationResult("metric", 0.3, "fallback")
