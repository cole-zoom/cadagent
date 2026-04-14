from dataclasses import dataclass


@dataclass
class IngestConfig:
    batch_size: int = 50
    max_file_size_mb: int = 500
    retry_count: int = 3
    rate_limit_delay: float = 0.5
