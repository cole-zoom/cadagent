"""Quality scoring for fact observations.

Assigns a confidence score and issue codes to each observation based on
mapping confidence, value plausibility, and completeness.
"""

import logging
from dataclasses import dataclass, field

from shared.models.observation import FactObservation

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    observation_id: str
    quality_confidence: float
    issue_codes: list[str] = field(default_factory=list)
    issue_notes: list[str] = field(default_factory=list)

    def to_bq_row(self) -> dict:
        return {
            "observation_id": self.observation_id,
            "quality_confidence": self.quality_confidence,
            "issue_codes": ",".join(self.issue_codes),
            "issue_notes": "; ".join(self.issue_notes),
            "review_status": "unreviewed",
            "reviewed_by": None,
            "reviewed_at": None,
        }


def score_observation(
    obs: FactObservation,
    mapping_confidence: float | None = None,
) -> QualityResult:
    """Score a single observation for quality issues."""
    score = 1.0
    issues: list[str] = []
    notes: list[str] = []

    # Missing metric
    if not obs.metric_id:
        score -= 0.3
        issues.append("MISSING_METRIC")
        notes.append("No metric ID assigned")

    # Missing time
    if not obs.time_id:
        score -= 0.2
        issues.append("MISSING_TIME")
        notes.append("No time dimension assigned")

    # Low mapping confidence
    if mapping_confidence is not None and mapping_confidence < 0.8:
        score -= 0.2
        issues.append("LOW_METRIC_CONFIDENCE")
        notes.append(f"Mapping confidence {mapping_confidence:.2f} below threshold")

    # Missing value
    if obs.value_numeric is None and not obs.value_text:
        score -= 0.3
        issues.append("MISSING_VALUE")
        notes.append("Neither numeric nor text value present")

    # Implausible values
    if obs.value_numeric is not None:
        if abs(obs.value_numeric) > 1e15:
            score -= 0.2
            issues.append("IMPLAUSIBLE_VALUE")
            notes.append(f"Value {obs.value_numeric} exceeds plausibility threshold")

    # Missing unit when value is numeric
    if obs.value_numeric is not None and not obs.unit_raw:
        score -= 0.05
        issues.append("MISSING_UNIT")

    score = max(0.0, min(1.0, score))

    return QualityResult(
        observation_id=obs.observation_id,
        quality_confidence=score,
        issue_codes=issues,
        issue_notes=notes,
    )
