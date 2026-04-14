"""Tests for services/normalize/run.py."""

from unittest.mock import MagicMock

from services.normalize.config import NormalizeConfig
from services.normalize.run import normalize_batch


class TestNormalizeBatch:
    def test_empty_tables_returns_zero_stats(self, mock_bq_client):
        """When query returns no tables to normalize, stats are all zero."""
        mock_bq_client.query.return_value = []

        config = NormalizeConfig()
        stats = normalize_batch(
            bq_client=mock_bq_client,
            config=config,
            project_id="test-project",
            stg_dataset="stg",
            cur_dataset="cur",
            raw_dataset="raw",
        )

        assert stats["tables_processed"] == 0
        assert stats["observations_created"] == 0
        assert stats["headers_classified"] == 0
        assert stats["errors"] == 0

    def test_query_called_with_department_filter(self, mock_bq_client):
        """When department_id is provided, the SQL should include a filter."""
        mock_bq_client.query.return_value = []

        config = NormalizeConfig()
        normalize_batch(
            bq_client=mock_bq_client,
            config=config,
            project_id="test-project",
            stg_dataset="stg",
            cur_dataset="cur",
            raw_dataset="raw",
            department_id="fin",
        )

        call_args = mock_bq_client.query.call_args
        sql = call_args[0][0]
        assert "department_id = 'fin'" in sql

    def test_query_called_without_department_filter(self, mock_bq_client):
        """When no department_id, no department filter in SQL."""
        mock_bq_client.query.return_value = []

        config = NormalizeConfig()
        normalize_batch(
            bq_client=mock_bq_client,
            config=config,
            project_id="test-project",
            stg_dataset="stg",
            cur_dataset="cur",
            raw_dataset="raw",
        )

        call_args = mock_bq_client.query.call_args
        sql = call_args[0][0]
        assert "AND et.department_id = " not in sql

    def test_table_processing_error_counted(self, mock_bq_client):
        """If _normalize_table raises, error is counted in stats."""
        mock_bq_client.query.side_effect = [
            # First call: tables query returns one table
            [{"table_id": "t1", "document_id": "d1", "department_id": "fin", "title": "Test"}],
            # Second call: headers query raises exception
            Exception("BQ error"),
        ]

        config = NormalizeConfig()
        stats = normalize_batch(
            bq_client=mock_bq_client,
            config=config,
            project_id="test-project",
            stg_dataset="stg",
            cur_dataset="cur",
            raw_dataset="raw",
        )

        assert stats["errors"] == 1
        assert stats["tables_processed"] == 0

    def test_uses_correct_dataset_references(self, mock_bq_client):
        """Verify the SQL references the correct project and datasets."""
        mock_bq_client.query.return_value = []

        config = NormalizeConfig()
        normalize_batch(
            bq_client=mock_bq_client,
            config=config,
            project_id="my-project",
            stg_dataset="my_stg",
            cur_dataset="my_cur",
            raw_dataset="my_raw",
        )

        call_args = mock_bq_client.query.call_args
        sql = call_args[0][0]
        assert "my-project.my_raw.extracted_tables" in sql
        assert "my-project.my_raw.documents" in sql
        assert "my-project.my_cur.fact_observation" in sql
