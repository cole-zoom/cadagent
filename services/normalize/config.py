from dataclasses import dataclass


@dataclass
class NormalizeConfig:
    min_classification_confidence: float = 0.5
    auto_approve_threshold: float = 0.8
    batch_size: int = 100
    max_observations_per_table: int = 50000
