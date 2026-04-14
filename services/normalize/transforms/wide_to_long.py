"""Transform strategies for converting extracted tables into fact observations.

Three department-specific strategies handle the fundamentally different
data shapes across fin, statcan, and tbs-sct.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from shared.models.observation import FactObservation

from ..classifiers.header_classifier import ClassificationResult, HeaderClassifier
from ..mappers.mapping_resolver import MappingResolver, ResolvedMapping

logger = logging.getLogger(__name__)


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


class StatcanTransformStrategy(TransformStrategy):
    """Statistics Canada: SDMX semi-structured observations.

    Columns are @OBS_VALUE, @TIME_PERIOD, @Geography, and various @Dimension attributes.
    Each row is already one observation.
    """

    def transform(
        self,
        headers: list[str],
        rows: list[list[str | None]],
        department_id: str,
        document_id: str,
        table_id: str,
    ) -> list[FactObservation]:
        # Find key column indices
        header_lower = [h.lower().strip() for h in headers]

        obs_value_idx = _find_col(header_lower, ["@obs_value", "value"])
        time_idx = _find_col(header_lower, ["@time_period", "ref_date"])
        geo_idx = _find_col(header_lower, ["@geography", "geo"])

        if obs_value_idx is None or time_idx is None:
            return []

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
