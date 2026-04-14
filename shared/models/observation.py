from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class FactObservation:
    observation_id: str
    department_id: str
    document_id: str
    table_id: str
    metric_id: str
    time_id: str
    geography_id: str | None = None
    scenario_id: str | None = None
    value_numeric: float | None = None
    value_text: str | None = None
    unit_raw: str | None = None
    scale_factor: float = 1.0
    currency_code: str | None = None
    source_row_number: int | None = None
    source_column_number: int | None = None
    quality_score: float | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_bq_row(self) -> dict:
        return {
            "observation_id": self.observation_id,
            "department_id": self.department_id,
            "document_id": self.document_id,
            "table_id": self.table_id,
            "metric_id": self.metric_id,
            "time_id": self.time_id,
            "geography_id": self.geography_id,
            "scenario_id": self.scenario_id,
            "value_numeric": self.value_numeric,
            "value_text": self.value_text,
            "unit_raw": self.unit_raw,
            "scale_factor": self.scale_factor,
            "currency_code": self.currency_code,
            "source_row_number": self.source_row_number,
            "source_column_number": self.source_column_number,
            "quality_score": self.quality_score,
            "created_at": self.created_at,
        }
