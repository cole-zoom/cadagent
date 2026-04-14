"""Tests for shared/clients/bigquery.py."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from shared.clients.bigquery import BigQueryClient


@pytest.fixture
def mock_bq_module():
    with patch("shared.clients.bigquery.bigquery") as mock_bq:
        mock_client_instance = MagicMock()
        mock_bq.Client.return_value = mock_client_instance
        mock_bq.ScalarQueryParameter = MagicMock()
        mock_bq.QueryJobConfig = MagicMock
        yield mock_bq, mock_client_instance


@pytest.fixture
def bq_client(mock_bq_module):
    _, _ = mock_bq_module
    return BigQueryClient(project_id="test-project")


class TestInsertRows:
    def test_calls_insert_rows_json(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_client.insert_rows_json.return_value = []

        rows = [{"col1": "val1"}, {"col1": "val2"}]
        errors = bq_client.insert_rows(dataset="raw", table="documents", rows=rows)

        mock_client.insert_rows_json.assert_called_once_with(
            "test-project.raw.documents", rows
        )
        assert errors == []

    def test_correct_table_ref(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_client.insert_rows_json.return_value = []

        bq_client.insert_rows(dataset="stg", table="headers", rows=[{"a": "b"}])

        call_args = mock_client.insert_rows_json.call_args[0]
        assert call_args[0] == "test-project.stg.headers"

    def test_returns_errors(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        error_list = [{"index": 0, "errors": [{"reason": "invalid"}]}]
        mock_client.insert_rows_json.return_value = error_list

        errors = bq_client.insert_rows(dataset="raw", table="documents", rows=[{"a": "b"}])
        assert errors == error_list


class TestQuery:
    def test_runs_query(self, bq_client, mock_bq_module):
        mock_bq, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_row = MagicMock()
        mock_row.__iter__ = MagicMock(return_value=iter([("key", "value")]))
        mock_row.keys.return_value = ["key"]
        mock_job.result.return_value = [mock_row]
        mock_client.query.return_value = mock_job

        # Use dict(row) compatible mock
        mock_dict_row = {"source_url": "https://example.com"}
        mock_job.result.return_value = [mock_dict_row]

        results = bq_client.query("SELECT * FROM table", params=None)
        mock_client.query.assert_called_once()

    def test_with_params(self, bq_client, mock_bq_module):
        mock_bq, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        bq_client.query("SELECT * FROM t WHERE id = @id", params={"id": "abc"})

        # Verify ScalarQueryParameter was constructed
        mock_bq.ScalarQueryParameter.assert_called_with("id", "STRING", "abc")

    def test_no_params(self, bq_client, mock_bq_module):
        mock_bq, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        bq_client.query("SELECT 1")
        # ScalarQueryParameter should NOT be called when no params
        mock_bq.ScalarQueryParameter.assert_not_called()


class TestTableExists:
    def test_returns_true(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_client.get_table.return_value = MagicMock()

        assert bq_client.table_exists("raw", "documents") is True
        mock_client.get_table.assert_called_once_with("test-project.raw.documents")

    def test_returns_false_on_exception(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_client.get_table.side_effect = Exception("Not found")

        assert bq_client.table_exists("raw", "documents") is False


class TestExecuteDdl:
    def test_executes_sql(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        bq_client.execute_ddl("CREATE TABLE test (id STRING)")
        mock_client.query.assert_called_once_with("CREATE TABLE test (id STRING)")
        mock_job.result.assert_called_once()


class TestExecuteDdlFile:
    def test_reads_and_executes_file(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE {project}.raw.test (id STRING)")
            f.flush()
            path = Path(f.name)

        bq_client.execute_ddl_file(path, replacements={"{project}": "test-project"})
        mock_client.query.assert_called_once_with(
            "CREATE TABLE test-project.raw.test (id STRING)"
        )
        path.unlink()

    def test_no_replacements(self, bq_client, mock_bq_module):
        _, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_client.query.return_value = mock_job

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE raw.test (id STRING)")
            f.flush()
            path = Path(f.name)

        bq_client.execute_ddl_file(path)
        mock_client.query.assert_called_once_with("CREATE TABLE raw.test (id STRING)")
        path.unlink()


class TestGetExistingSourceUrls:
    def test_returns_set_of_urls(self, bq_client, mock_bq_module):
        mock_bq, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_job.result.return_value = [
            {"source_url": "https://example.com/a.csv"},
            {"source_url": "https://example.com/b.csv"},
        ]
        mock_client.query.return_value = mock_job

        urls = bq_client.get_existing_source_urls("raw", "dept1")
        assert isinstance(urls, set)
        assert "https://example.com/a.csv" in urls
        assert "https://example.com/b.csv" in urls

    def test_empty_result(self, bq_client, mock_bq_module):
        mock_bq, mock_client = mock_bq_module
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        urls = bq_client.get_existing_source_urls("raw", "dept1")
        assert urls == set()
