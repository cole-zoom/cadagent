from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    gcp_project_id: str = ""
    gcp_region: str = "us-east1"

    gcs_raw_bucket: str = ""
    gcs_processed_bucket: str = ""

    bq_raw_dataset: str = "raw"
    bq_stg_dataset: str = "stg"
    bq_cur_dataset: str = "cur"
    bq_quality_dataset: str = "quality"

    goc_api_base_url: str = "https://open.canada.ca/data/en/api/3/action"
    target_departments: str = "fin,statcan,tbs-sct"

    log_level: str = "INFO"

    @property
    def departments_list(self) -> list[str]:
        return [d.strip() for d in self.target_departments.split(",") if d.strip()]


settings = Settings()
