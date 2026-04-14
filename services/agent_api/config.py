from dataclasses import dataclass


@dataclass
class AgentConfig:
    llm_model: str = "claude-sonnet-4-20250514"
    max_sql_rows: int = 1000
    max_retries: int = 2
    bq_timeout_seconds: int = 30
