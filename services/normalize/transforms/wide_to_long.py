"""Transform strategies for converting extracted tables into fact observations.

Three department-specific strategies handle the fundamentally different
data shapes across fin, statcan, and tbs-sct.
"""

import hashlib
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import yaml

from shared.models.observation import FactObservation

from ..classifiers.header_classifier import ClassificationResult, HeaderClassifier
from ..mappers.mapping_resolver import MappingResolver, ResolvedMapping

logger = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "mappings"

# Lazy-loaded Finance dimensional column config (see mappings/finance_dimensions.yaml)
_FIN_DIMENSIONS: dict | None = None


def _load_fin_dimensions() -> dict:
    global _FIN_DIMENSIONS
    if _FIN_DIMENSIONS is not None:
        return _FIN_DIMENSIONS
    path = MAPPINGS_DIR / "finance_dimensions.yaml"
    if path.exists():
        _FIN_DIMENSIONS = yaml.safe_load(path.read_text()) or {}
    else:
        _FIN_DIMENSIONS = {}
    return _FIN_DIMENSIONS


# Year embedded inside a header string, e.g. "Population, 2016" or "POP2006"
YEAR_IN_HEADER_RE = re.compile(r"(19|20)\d{2}")

# Keywords that mark a census-wide column as numeric measure rather than a geo ID
CENSUS_MEASURE_KEYWORDS = (
    "pop", "density", "income", "dwelling", "household", "area",
    "land", "count", "total", "median", "average", "wage", "earning",
    "employ", "rate", "ratio", "percent",
)

# Common StatCan geographic-identifier column prefixes (lowercased)
STATCAN_GEO_ID_COLUMNS = ("dauid", "hruid", "csduid", "pruid", "cduid", "cmauid", "dguid")


class TransformStrategy(ABC):
    def __init__(self, classifier: HeaderClassifier, resolver: MappingResolver):
        self.classifier = classifier
        self.resolver = resolver

    @abstractmethod
    def transform(
        self,
        headers: list[str],
        rows: list[list[str | None]],
        department_id: str,
        document_id: str,
        table_id: str,
    ) -> list[FactObservation]:
        ...


class FinanceTransformStrategy(TransformStrategy):
    """Finance Canada: wide tables with time-as-columns, metric-as-rows.

    Example:
        | [metric label] | 2023-24 | 2024-25 | 2025-26 |
        | Revenues       | 100     | 110     | 120     |
    """

    def transform(
        self,
        headers: list[str],
        rows: list[list[str | None]],
        department_id: str,
        document_id: str,
        table_id: str,
    ) -> list[FactObservation]:
        # Classify all headers
        classifications: list[tuple[int, ClassificationResult, ResolvedMapping]] = []
        for i, h in enumerate(headers):
            cls_result = self.classifier.classify(h)
            mapping = self.resolver.resolve(h, cls_result.header_class, cls_result.canonical_hint)
            classifications.append((i, cls_result, mapping))

        # Find time columns and the label column (first column or metric column)
        time_columns = [(i, cls, mp) for i, cls, mp in classifications if cls.header_class in ("time", "time_range")]
        geo_columns = [(i, cls, mp) for i, cls, mp in classifications if cls.header_class == "geography"]

        # Dimensional long-format: dedicated VALUE/amount column means each row
        # is already an observation dimensioned by the other categorical columns.
        header_lower = [h.lower().strip() for h in headers]
        value_idx = _find_col(header_lower, ["value", "amount", "montant"])
        if value_idx is not None:
            return self._transform_dimensional_long(
                headers, header_lower, rows, classifications,
                value_idx, time_columns, department_id, document_id, table_id,
            )

        # If headers are geography (provinces), this is a geo-as-columns table
        if len(geo_columns) > len(time_columns) and len(geo_columns) >= 2:
            return self._transform_geo_as_columns(
                headers, rows, classifications, department_id, document_id, table_id
            )

        # Default: time-as-columns
        return self._transform_time_as_columns(
            headers, rows, classifications, time_columns, department_id, document_id, table_id
        )

    def _transform_time_as_columns(
        self, headers, rows, classifications, time_columns,
        department_id, document_id, table_id,
    ) -> list[FactObservation]:
        observations = []
        now = datetime.now(timezone.utc).isoformat()

        for row_idx, row in enumerate(rows):
            # First column is usually the metric label
            row_label = str(row[0]).strip() if row and row[0] else None
            if not row_label:
                continue

            row_cls = self.classifier.classify(row_label)
            row_mapping = self.resolver.resolve(row_label, row_cls.header_class, row_cls.canonical_hint)

            if row_cls.header_class == "junk" or not row_mapping.metric_id:
                continue

            for col_idx, col_cls, col_mapping in time_columns:
                if col_idx >= len(row):
                    continue

                value_raw = row[col_idx]
                value_numeric = _try_numeric(value_raw)
                if value_numeric is None and not value_raw:
                    continue

                obs_id = hashlib.sha256(
                    f"{table_id}|{row_idx}|{col_idx}".encode()
                ).hexdigest()[:32]

                observations.append(FactObservation(
                    observation_id=obs_id,
                    department_id=department_id,
                    document_id=document_id,
                    table_id=table_id,
                    metric_id=row_mapping.metric_id,
                    time_id=col_mapping.time_id or "",
                    geography_id=None,
                    scenario_id=col_mapping.scenario_id,
                    value_numeric=value_numeric,
                    value_text=str(value_raw).strip() if value_raw and value_numeric is None else None,
                    source_row_number=row_idx,
                    source_column_number=col_idx,
                    created_at=now,
                ))

        return observations

    def _transform_geo_as_columns(
        self, headers, rows, classifications,
        department_id, document_id, table_id,
    ) -> list[FactObservation]:
        """Handle tables like Year | NL | PE | ... | Total Canada."""
        observations = []
        now = datetime.now(timezone.utc).isoformat()

        geo_cols = [(i, cls, mp) for i, cls, mp in classifications if cls.header_class == "geography"]

        for row_idx, row in enumerate(rows):
            # First column is typically Year
            row_label = str(row[0]).strip() if row and row[0] else None
            if not row_label:
                continue

            time_result = self.classifier.classify(row_label)
            time_mapping = self.resolver.resolve(row_label, time_result.header_class, time_result.canonical_hint)

            if not time_mapping.time_id:
                continue

            for col_idx, col_cls, col_mapping in geo_cols:
                if col_idx >= len(row):
                    continue

                value_raw = row[col_idx]
                value_numeric = _try_numeric(value_raw)
                if value_numeric is None:
                    continue

                obs_id = hashlib.sha256(
                    f"{table_id}|{row_idx}|{col_idx}".encode()
                ).hexdigest()[:32]

                observations.append(FactObservation(
                    observation_id=obs_id,
                    department_id=department_id,
                    document_id=document_id,
                    table_id=table_id,
                    metric_id="",  # Needs to come from table title or context
                    time_id=time_mapping.time_id,
                    geography_id=col_mapping.geography_id,
                    scenario_id=None,
                    value_numeric=value_numeric,
                    source_row_number=row_idx,
                    source_column_number=col_idx,
                    created_at=now,
                ))

        return observations

    def _transform_dimensional_long(
        self, headers, header_lower, rows, classifications,
        value_idx, time_columns, department_id, document_id, table_id,
    ) -> list[FactObservation]:
        """Long-format: one row per observation, dimensioned by categorical columns,
        with numeric value in the VALUE column. Handles hybrids with year-columns too.
        """
        observations = []
        now = datetime.now(timezone.utc).isoformat()

        dim_cfg = _load_fin_dimensions()
        metric_label_cols_cfg = set(dim_cfg.get("metric_label_columns", []))
        # Indices of columns we use to build a composite metric label
        metric_label_indices = [
            i for i, h in enumerate(header_lower) if h in metric_label_cols_cfg
        ]

        time_cols_excluding_value = [
            (ci, cls, mp) for ci, cls, mp in time_columns if ci != value_idx
        ]

        for row_idx, row in enumerate(rows):
            # Build a composite metric label from the configured label columns.
            # Fallback: first column's value if no label columns matched.
            metric_parts = []
            for col_idx in metric_label_indices:
                if col_idx < len(row) and row[col_idx] and str(row[col_idx]).strip():
                    metric_parts.append(str(row[col_idx]).strip())
            metric_label = " | ".join(metric_parts) if metric_parts else (
                str(row[0]).strip() if row and row[0] else None
            )
            if not metric_label:
                continue

            metric_cls = self.classifier.classify(metric_label)
            if metric_cls.header_class == "junk":
                continue
            metric_mapping = self.resolver.resolve(
                metric_label, metric_cls.header_class, metric_cls.canonical_hint,
            )
            if not metric_mapping.metric_id:
                continue

            # Observation from the VALUE column itself. If there are time columns,
            # attribute it to the first one; otherwise time_id is empty.
            value_raw = row[value_idx] if value_idx < len(row) else None
            value_numeric = _try_numeric(value_raw)
            if value_numeric is not None:
                time_id = ""
                scenario_id = None
                if time_cols_excluding_value:
                    _, _, first_time_mp = time_cols_excluding_value[0]
                    time_id = first_time_mp.time_id or ""
                    scenario_id = first_time_mp.scenario_id

                obs_id = hashlib.sha256(
                    f"{table_id}|{row_idx}|{value_idx}".encode()
                ).hexdigest()[:32]
                observations.append(FactObservation(
                    observation_id=obs_id,
                    department_id=department_id,
                    document_id=document_id,
                    table_id=table_id,
                    metric_id=metric_mapping.metric_id,
                    time_id=time_id,
                    scenario_id=scenario_id,
                    value_numeric=value_numeric,
                    source_row_number=row_idx,
                    source_column_number=value_idx,
                    created_at=now,
                ))

            # Hybrid tables also have year-columns with numeric values alongside VALUE
            for col_idx, col_cls, col_mapping in time_cols_excluding_value:
                if col_idx >= len(row):
                    continue
                cell_value = row[col_idx]
                cell_numeric = _try_numeric(cell_value)
                if cell_numeric is None:
                    continue

                obs_id = hashlib.sha256(
                    f"{table_id}|{row_idx}|{col_idx}".encode()
                ).hexdigest()[:32]
                observations.append(FactObservation(
                    observation_id=obs_id,
                    department_id=department_id,
                    document_id=document_id,
                    table_id=table_id,
                    metric_id=metric_mapping.metric_id,
                    time_id=col_mapping.time_id or "",
                    scenario_id=col_mapping.scenario_id,
                    value_numeric=cell_numeric,
                    source_row_number=row_idx,
                    source_column_number=col_idx,
                    created_at=now,
                ))

        return observations


class StatcanTransformStrategy(TransformStrategy):
    """Statistics Canada: routes to the right sub-strategy by data shape.

    Three shapes exist in practice:
      1. SDMX / CANSIM long-format  — @OBS_VALUE/VALUE + @TIME_PERIOD/REF_DATE columns.
         Each row is already an observation.
      2. Census-style wide tables   — year embedded in column names (POP2006,
         "Population, 2016"). Rows are geographic units; columns unpivot into
         (metric, year) observations.
      3. Classification / taxonomy  — Level/Code/Class title/Hierarchical structure.
         Reference data with no numeric observations. Returns [].
    """

    def transform(
        self,
        headers: list[str],
        rows: list[list[str | None]],
        department_id: str,
        document_id: str,
        table_id: str,
    ) -> list[FactObservation]:
        header_lower = [h.lower().strip() for h in headers]

        # Shape 1: SDMX / CANSIM long-format
        obs_value_idx = _find_col(header_lower, ["@obs_value", "value"])
        time_idx = _find_col(header_lower, ["@time_period", "ref_date"])
        if obs_value_idx is not None and time_idx is not None:
            return self._transform_long_format(
                headers, header_lower, rows, obs_value_idx, time_idx,
                department_id, document_id, table_id,
            )

        # Shape 2: census-style wide (year embedded in header names)
        year_columns = self._detect_year_columns(headers)
        if year_columns:
            return self._transform_census_wide(
                headers, header_lower, rows, year_columns,
                department_id, document_id, table_id,
            )

        # Shape 3: classification/taxonomy — no observations
        return []

    def _transform_long_format(
        self, headers, header_lower, rows, obs_value_idx, time_idx,
        department_id, document_id, table_id,
    ) -> list[FactObservation]:
        geo_idx = _find_col(header_lower, ["@geography", "geo", "dguid"])

        observations = []
        now = datetime.now(timezone.utc).isoformat()

        for row_idx, row in enumerate(rows):
            value_raw = row[obs_value_idx] if obs_value_idx < len(row) else None
            value_numeric = _try_numeric(value_raw)

            time_raw = str(row[time_idx]).strip() if time_idx < len(row) and row[time_idx] else None
            if not time_raw:
                continue

            time_mapping = self.resolver.resolve(time_raw, "time", None)
            geo_id = None
            if geo_idx is not None and geo_idx < len(row) and row[geo_idx]:
                geo_mapping = self.resolver.resolve(str(row[geo_idx]).strip(), "geography", None)
                geo_id = geo_mapping.geography_id

            obs_id = hashlib.sha256(
                f"{table_id}|{row_idx}".encode()
            ).hexdigest()[:32]

            observations.append(FactObservation(
                observation_id=obs_id,
                department_id=department_id,
                document_id=document_id,
                table_id=table_id,
                metric_id="",  # Resolved from dataset title context
                time_id=time_mapping.time_id or "",
                geography_id=geo_id,
                value_numeric=value_numeric,
                value_text=str(value_raw).strip() if value_raw and value_numeric is None else None,
                source_row_number=row_idx,
                created_at=now,
            ))

        return observations

    def _detect_year_columns(self, headers: list[str]) -> list[tuple[int, str]]:
        """Return [(col_idx, year)] for columns that embed a year AND look like a
        numeric measure. Excludes geographic-ID columns that happen to have years
        (e.g. DAUID2006)."""
        year_cols = []
        for i, h in enumerate(headers):
            match = YEAR_IN_HEADER_RE.search(h)
            if not match:
                continue
            h_lower = h.lower()
            # Exclude known geo-ID columns even though they contain a year
            if any(h_lower.startswith(prefix) for prefix in STATCAN_GEO_ID_COLUMNS):
                continue
            if any(kw in h_lower for kw in CENSUS_MEASURE_KEYWORDS):
                year_cols.append((i, match.group(0)))
        return year_cols

    def _transform_census_wide(
        self, headers, header_lower, rows, year_columns,
        department_id, document_id, table_id,
    ) -> list[FactObservation]:
        """Unpivot census-wide tables: one observation per (row, year-column) pair."""
        observations = []
        now = datetime.now(timezone.utc).isoformat()

        # Geographic-ID column for this row. First match wins.
        geo_idx = None
        for i, h in enumerate(header_lower):
            if any(prefix in h for prefix in STATCAN_GEO_ID_COLUMNS):
                geo_idx = i
                break

        for row_idx, row in enumerate(rows):
            geo_id = None
            if geo_idx is not None and geo_idx < len(row) and row[geo_idx]:
                geo_raw = str(row[geo_idx]).strip()
                if geo_raw:
                    geo_mapping = self.resolver.resolve(geo_raw, "geography", None)
                    geo_id = geo_mapping.geography_id

            for col_idx, year in year_columns:
                if col_idx >= len(row):
                    continue
                value_raw = row[col_idx]
                value_numeric = _try_numeric(value_raw)
                if value_numeric is None:
                    continue

                time_mapping = self.resolver.resolve(year, "time", None)

                # Strip the year from the header to get the metric label
                metric_label = YEAR_IN_HEADER_RE.sub("", headers[col_idx])
                metric_label = re.sub(r"[,.]?\s+$", "", metric_label).strip(" ,.")
                if not metric_label:
                    metric_label = headers[col_idx]
                metric_cls = self.classifier.classify(metric_label)
                metric_mapping = self.resolver.resolve(
                    metric_label, metric_cls.header_class, metric_cls.canonical_hint,
                )

                obs_id = hashlib.sha256(
                    f"{table_id}|{row_idx}|{col_idx}".encode()
                ).hexdigest()[:32]

                observations.append(FactObservation(
                    observation_id=obs_id,
                    department_id=department_id,
                    document_id=document_id,
                    table_id=table_id,
                    metric_id=metric_mapping.metric_id or "",
                    time_id=time_mapping.time_id or "",
                    geography_id=geo_id,
                    value_numeric=value_numeric,
                    source_row_number=row_idx,
                    source_column_number=col_idx,
                    created_at=now,
                ))

        return observations


class TbsSctTransformStrategy(TransformStrategy):
    """Treasury Board Secretariat: heterogeneous formats.

    Handles estimates/budgetary data, survey data, COVID expenditure tracking.
    Each row is typically already a single observation.
    """

    def transform(
        self,
        headers: list[str],
        rows: list[list[str | None]],
        department_id: str,
        document_id: str,
        table_id: str,
    ) -> list[FactObservation]:
        header_lower = [h.lower().strip() for h in headers]

        # Detect which tbs-sct format this is
        if "amount" in header_lower or "expenditure" in header_lower:
            return self._transform_financial(headers, header_lower, rows, department_id, document_id, table_id)

        if "score100" in header_lower or "score5" in header_lower:
            return self._transform_survey(headers, header_lower, rows, department_id, document_id, table_id)

        # Fallback: try generic column-value extraction
        return self._transform_generic(headers, header_lower, rows, department_id, document_id, table_id)

    def _transform_financial(
        self, headers, header_lower, rows, department_id, document_id, table_id,
    ) -> list[FactObservation]:
        value_idx = _find_col(header_lower, ["amount", "expenditure", "total"])
        if value_idx is None:
            return []

        observations = []
        now = datetime.now(timezone.utc).isoformat()

        for row_idx, row in enumerate(rows):
            value_raw = row[value_idx] if value_idx < len(row) else None
            value_numeric = _try_numeric(value_raw)
            if value_numeric is None:
                continue

            obs_id = hashlib.sha256(
                f"{table_id}|{row_idx}".encode()
            ).hexdigest()[:32]

            observations.append(FactObservation(
                observation_id=obs_id,
                department_id=department_id,
                document_id=document_id,
                table_id=table_id,
                metric_id="",
                time_id="",
                value_numeric=value_numeric,
                source_row_number=row_idx,
                created_at=now,
            ))

        return observations

    def _transform_survey(
        self, headers, header_lower, rows, department_id, document_id, table_id,
    ) -> list[FactObservation]:
        score_idx = _find_col(header_lower, ["score100", "score5"])
        if score_idx is None:
            return []

        observations = []
        now = datetime.now(timezone.utc).isoformat()

        for row_idx, row in enumerate(rows):
            value_raw = row[score_idx] if score_idx < len(row) else None
            value_numeric = _try_numeric(value_raw)
            if value_numeric is None:
                continue

            obs_id = hashlib.sha256(
                f"{table_id}|survey|{row_idx}".encode()
            ).hexdigest()[:32]

            observations.append(FactObservation(
                observation_id=obs_id,
                department_id=department_id,
                document_id=document_id,
                table_id=table_id,
                metric_id="",
                time_id="",
                value_numeric=value_numeric,
                source_row_number=row_idx,
                created_at=now,
            ))

        return observations

    def _transform_generic(
        self, headers, header_lower, rows, department_id, document_id, table_id,
    ) -> list[FactObservation]:
        # Find any numeric-looking column
        return []


def get_strategy(
    department_id: str, classifier: HeaderClassifier, resolver: MappingResolver
) -> TransformStrategy:
    """Get the appropriate transform strategy for a department."""
    strategies = {
        "fin": FinanceTransformStrategy,
        "statcan": StatcanTransformStrategy,
        "tbs-sct": TbsSctTransformStrategy,
    }
    strategy_cls = strategies.get(department_id, FinanceTransformStrategy)
    return strategy_cls(classifier, resolver)


def _find_col(header_lower: list[str], candidates: list[str]) -> int | None:
    for candidate in candidates:
        if candidate in header_lower:
            return header_lower.index(candidate)
    return None


def _try_numeric(s: str | None) -> float | None:
    if not s:
        return None
    cleaned = str(s).replace(",", "").replace(" ", "").replace("$", "").replace("%", "").strip()
    if cleaned in ("", "-", "...", "x", "n/a", "na", "F", "E", "None"):
        return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None
