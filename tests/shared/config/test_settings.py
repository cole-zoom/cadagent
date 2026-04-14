"""Tests for shared/config/settings.py."""

import os

import pytest

from shared.config.settings import Settings


class TestSettings:
    def test_default_bq_raw_dataset(self, monkeypatch):
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GCS_RAW_BUCKET", "test-raw")
        monkeypatch.setenv("GCS_PROCESSED_BUCKET", "test-processed")
        s = Settings(
            gcp_project_id="test-project",
            gcs_raw_bucket="test-raw",
            gcs_processed_bucket="test-processed",
        )
        assert s.bq_raw_dataset == "raw"

    def test_default_bq_stg_dataset(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.bq_stg_dataset == "stg"

    def test_default_bq_cur_dataset(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.bq_cur_dataset == "cur"

    def test_default_bq_quality_dataset(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.bq_quality_dataset == "quality"

    def test_default_goc_api_base_url(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.goc_api_base_url.startswith("https://open.canada.ca")

    def test_default_gcp_region(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.gcp_region == "us-east1"

    def test_default_log_level(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert s.log_level == "INFO"

    def test_default_target_departments(self):
        s = Settings(
            gcp_project_id="test",
            gcs_raw_bucket="test",
            gcs_processed_bucket="test",
        )
        assert "fin" in s.target_departments
        assert "statcan" in s.target_departments

    def test_override_via_constructor(self):
        s = Settings(
            gcp_project_id="my-project",
            gcs_raw_bucket="my-raw-bucket",
            gcs_processed_bucket="my-processed-bucket",
            bq_raw_dataset="custom_raw",
        )
        assert s.gcp_project_id == "my-project"
        assert s.bq_raw_dataset == "custom_raw"

    def test_override_via_env(self, monkeypatch):
        monkeypatch.setenv("GCP_PROJECT_ID", "env-project")
        monkeypatch.setenv("GCS_RAW_BUCKET", "env-raw")
        monkeypatch.setenv("GCS_PROCESSED_BUCKET", "env-processed")
        monkeypatch.setenv("BQ_RAW_DATASET", "env_raw_ds")
        s = Settings()
        assert s.gcp_project_id == "env-project"
        assert s.bq_raw_dataset == "env_raw_ds"
