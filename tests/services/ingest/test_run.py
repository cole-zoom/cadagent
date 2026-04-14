"""Tests for services/ingest/run.py."""

from unittest.mock import MagicMock, patch

import pytest

from services.ingest.config import IngestConfig
from services.ingest.run import infer_document_type, extract_year_from_title, ingest_department


class TestInferDocumentType:
    def test_budget(self):
        assert infer_document_type("Federal Budget 2025") == "budget"

    def test_estimates(self):
        assert infer_document_type("Supplementary Estimates") == "estimates"

    def test_fiscal_monitor(self):
        assert infer_document_type("The Fiscal Monitor: 2025") == "fiscal_monitor"

    def test_economic_update(self):
        assert infer_document_type("Economic and Fiscal Update 2024") == "economic_update"

    def test_economic_statement(self):
        assert infer_document_type("Economic Statement 2024") == "economic_update"

    def test_economic_survey(self):
        assert infer_document_type("Survey of Private Sector Economic Forecasters") == "economic_survey"

    def test_transfer_tables(self):
        assert infer_document_type("Historical Transfer Tables") == "transfer_tables"

    def test_proactive_disclosure(self):
        assert infer_document_type("Proactive Disclosure Reports") == "proactive_disclosure"

    def test_employee_survey(self):
        assert infer_document_type("Public Service Employee Survey Results") == "employee_survey"

    def test_pses(self):
        assert infer_document_type("PSES 2023 Data") == "employee_survey"

    def test_expenditure_tracking_infobase(self):
        assert infer_document_type("GC InfoBase Dataset") == "expenditure_tracking"

    def test_expenditure_tracking_covid(self):
        assert infer_document_type("COVID-19 Spending") == "expenditure_tracking"

    def test_international_aid(self):
        assert infer_document_type("International Aid Contributions") == "international_aid"

    def test_iati(self):
        assert infer_document_type("IATI Data Release") == "international_aid"

    def test_other(self):
        assert infer_document_type("random title") == "other"

    def test_case_insensitive(self):
        assert infer_document_type("FEDERAL BUDGET 2025") == "budget"


class TestExtractYearFromTitle:
    def test_year_present(self):
        assert extract_year_from_title("Federal Budget 2025") == "2025"

    def test_no_year(self):
        assert extract_year_from_title("No year here") is None

    def test_first_year_extracted(self):
        assert extract_year_from_title("Budget 2023-2024") == "2023"

    def test_year_at_start(self):
        assert extract_year_from_title("2024 Estimates") == "2024"

    def test_no_match_for_19xx(self):
        assert extract_year_from_title("Data from 1999") is None


class TestIngestDepartment:
    def _make_mocks(self):
        api_client = MagicMock()
        gcs_client = MagicMock()
        bq_client = MagicMock()
        config = IngestConfig()

        dataset = {
            "title": "Federal Budget 2025",
            "title_translated": {"en": "Federal Budget 2025"},
            "resources": [
                {
                    "url": "https://open.canada.ca/data/budget.csv",
                    "name": "budget_data",
                    "format": "CSV",
                    "language": "en",
                }
            ],
            "date_published": "2025-04-01T00:00:00",
        }

        api_client.search_all_datasets.return_value = [dataset]
        api_client.list_resources.return_value = dataset["resources"]
        api_client.download_resource.return_value = b"test data"
        bq_client.get_existing_source_urls.return_value = set()
        bq_client.insert_rows.return_value = []
        gcs_client.upload_raw_file.return_value = "gs://bucket/raw/file.csv"

        return api_client, gcs_client, bq_client, config, dataset

    @patch("services.ingest.run.GocApiClient.extract_format", return_value="csv")
    @patch("services.ingest.run.GocApiClient.extract_language", return_value="en")
    @patch("services.ingest.run.GocApiClient.extract_title", return_value="Federal Budget 2025")
    def test_success_path(self, mock_title, mock_lang, mock_fmt):
        api_client, gcs_client, bq_client, config, dataset = self._make_mocks()

        stats = ingest_department(
            department_code="fin",
            mode="full",
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="test-bucket",
            raw_dataset="test_dataset",
        )

        assert stats["success"] == 1
        assert stats["failed"] == 0
        assert stats["skipped"] == 0
        assert stats["duplicate"] == 0
        bq_client.insert_rows.assert_called_once()
        gcs_client.upload_raw_file.assert_called_once()

    @patch("services.ingest.run.GocApiClient.extract_format", return_value="csv")
    @patch("services.ingest.run.GocApiClient.extract_language", return_value="en")
    @patch("services.ingest.run.GocApiClient.extract_title", return_value="Federal Budget 2025")
    def test_incremental_skips_existing(self, mock_title, mock_lang, mock_fmt):
        api_client, gcs_client, bq_client, config, dataset = self._make_mocks()
        bq_client.get_existing_source_urls.return_value = {
            "https://open.canada.ca/data/budget.csv"
        }

        stats = ingest_department(
            department_code="fin",
            mode="incremental",
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="test-bucket",
            raw_dataset="test_dataset",
        )

        assert stats["duplicate"] == 1
        assert stats["success"] == 0
        api_client.download_resource.assert_not_called()

    @patch("services.ingest.run.GocApiClient.extract_format", return_value="csv")
    @patch("services.ingest.run.GocApiClient.extract_language", return_value="en")
    @patch("services.ingest.run.GocApiClient.extract_title", return_value="Federal Budget 2025")
    def test_oversized_file_skipped(self, mock_title, mock_lang, mock_fmt):
        api_client, gcs_client, bq_client, config, dataset = self._make_mocks()
        config.max_file_size_mb = 0  # any file is too large

        stats = ingest_department(
            department_code="fin",
            mode="full",
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="test-bucket",
            raw_dataset="test_dataset",
        )

        assert stats["skipped"] == 1
        assert stats["success"] == 0
        gcs_client.upload_raw_file.assert_not_called()

    @patch("services.ingest.run.GocApiClient.extract_format", return_value="csv")
    @patch("services.ingest.run.GocApiClient.extract_language", return_value="en")
    @patch("services.ingest.run.GocApiClient.extract_title", return_value="Federal Budget 2025")
    def test_download_failure_counted_as_failed(self, mock_title, mock_lang, mock_fmt):
        api_client, gcs_client, bq_client, config, dataset = self._make_mocks()
        api_client.download_resource.side_effect = Exception("network error")

        stats = ingest_department(
            department_code="fin",
            mode="full",
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="test-bucket",
            raw_dataset="test_dataset",
        )

        assert stats["failed"] == 1
        assert stats["success"] == 0

    @patch("services.ingest.run.GocApiClient.extract_format", return_value="csv")
    @patch("services.ingest.run.GocApiClient.extract_language", return_value="en")
    @patch("services.ingest.run.GocApiClient.extract_title", return_value="Federal Budget 2025")
    def test_empty_url_skipped(self, mock_title, mock_lang, mock_fmt):
        api_client, gcs_client, bq_client, config, dataset = self._make_mocks()
        api_client.list_resources.return_value = [{"url": "", "name": "empty"}]

        stats = ingest_department(
            department_code="fin",
            mode="full",
            api_client=api_client,
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="test-bucket",
            raw_dataset="test_dataset",
        )

        assert stats["success"] == 0
        assert stats["failed"] == 0
        assert stats["skipped"] == 0
        api_client.download_resource.assert_not_called()
