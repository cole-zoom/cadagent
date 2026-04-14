"""Tests for shared/clients/goc_api.py."""

import json
from unittest.mock import MagicMock, patch

import pytest

from shared.clients.goc_api import GocApiClient


@pytest.fixture
def client():
    return GocApiClient(base_url="https://open.canada.ca/data/en/api/3/action", rate_limit_delay=0)


@pytest.fixture
def mock_session_get():
    with patch("requests.Session.get") as mock_get:
        yield mock_get


def _make_response(data, success=True):
    """Helper to create a mock response object."""
    resp = MagicMock()
    resp.json.return_value = {"success": success, "result": data}
    resp.raise_for_status.return_value = None
    return resp


class TestInit:
    def test_base_url_strip_trailing_slash(self):
        c = GocApiClient(base_url="https://example.com/api/", rate_limit_delay=0)
        assert c.base_url == "https://example.com/api"

    def test_rate_limit_delay(self):
        c = GocApiClient(base_url="https://example.com/api", rate_limit_delay=1.5)
        assert c.rate_limit_delay == 1.5

    def test_session_created(self):
        c = GocApiClient(base_url="https://example.com/api", rate_limit_delay=0)
        assert c.session is not None


class TestGet:
    def test_calls_session_get(self, client, mock_session_get):
        mock_session_get.return_value = _make_response({"key": "value"})
        result = client._get("test_action", params={"q": "test"})
        mock_session_get.assert_called_once()
        call_args = mock_session_get.call_args
        assert "test_action" in call_args[0][0]
        assert result == {"key": "value"}

    def test_api_error_raises_runtime_error(self, client, mock_session_get):
        resp = MagicMock()
        resp.json.return_value = {"success": False, "error": "bad request"}
        resp.raise_for_status.return_value = None
        mock_session_get.return_value = resp
        with pytest.raises(RuntimeError, match="CKAN API error"):
            client._get("test_action")


class TestSearchDatasets:
    def test_returns_tuple(self, client, mock_session_get):
        mock_session_get.return_value = _make_response({
            "results": [{"id": "ds1"}, {"id": "ds2"}],
            "count": 2,
        })
        results, count = client.search_datasets("fin")
        assert len(results) == 2
        assert count == 2

    def test_filter_query(self, client, mock_session_get):
        mock_session_get.return_value = _make_response({
            "results": [],
            "count": 0,
        })
        client.search_datasets("fin", fq="res_format:CSV")
        call_args = mock_session_get.call_args
        params = call_args[1]["params"] if "params" in call_args[1] else call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("params")
        assert "organization:fin+res_format:CSV" in params["fq"]


class TestSearchAllDatasets:
    @patch("shared.clients.goc_api.time.sleep")
    def test_paginates(self, mock_sleep, client, mock_session_get):
        # First page: 100 results out of 150 total
        first_page = _make_response({
            "results": [{"id": f"ds{i}"} for i in range(100)],
            "count": 150,
        })
        # Second page: 50 results
        second_page = _make_response({
            "results": [{"id": f"ds{i}"} for i in range(100, 150)],
            "count": 150,
        })
        mock_session_get.side_effect = [first_page, second_page]

        results = client.search_all_datasets("fin")
        assert len(results) == 150
        # sleep called between pages
        mock_sleep.assert_called()

    @patch("shared.clients.goc_api.time.sleep")
    def test_single_page(self, mock_sleep, client, mock_session_get):
        mock_session_get.return_value = _make_response({
            "results": [{"id": "ds1"}],
            "count": 1,
        })
        results = client.search_all_datasets("fin")
        assert len(results) == 1
        # sleep should not be called for single page
        mock_sleep.assert_not_called()


class TestGetDataset:
    def test_returns_dataset(self, client, mock_session_get):
        mock_session_get.return_value = _make_response({"id": "ds1", "title": "Test"})
        result = client.get_dataset("ds1")
        assert result["id"] == "ds1"


class TestDownloadResource:
    @patch("shared.clients.goc_api.time.sleep")
    def test_absolute_url(self, mock_sleep, client, mock_session_get):
        resp = MagicMock()
        resp.content = b"file data"
        resp.raise_for_status.return_value = None
        mock_session_get.return_value = resp

        result = client.download_resource("https://example.com/file.csv")
        assert result == b"file data"
        # Verify absolute URL was used as-is
        call_url = mock_session_get.call_args[0][0]
        assert call_url == "https://example.com/file.csv"

    @patch("shared.clients.goc_api.time.sleep")
    def test_relative_url(self, mock_sleep, client, mock_session_get):
        resp = MagicMock()
        resp.content = b"file data"
        resp.raise_for_status.return_value = None
        mock_session_get.return_value = resp

        result = client.download_resource("/dataset/abc/resource/file.csv")
        call_url = mock_session_get.call_args[0][0]
        assert call_url.startswith("https://open.canada.ca")


class TestExtractTitle:
    def test_translated_title(self):
        dataset = {"title_translated": {"en": "Budget 2024", "fr": "Budget 2024"}}
        assert GocApiClient.extract_title(dataset) == "Budget 2024"

    def test_fallback_to_title(self):
        dataset = {"title": "Fallback Title"}
        assert GocApiClient.extract_title(dataset) == "Fallback Title"

    def test_no_title(self):
        dataset = {}
        assert GocApiClient.extract_title(dataset) == ""

    def test_translated_missing_en(self):
        dataset = {"title_translated": {"fr": "Budget 2024"}, "title": "Fallback"}
        assert GocApiClient.extract_title(dataset) == "Fallback"

    def test_translated_not_dict(self):
        dataset = {"title_translated": "plain string", "title": "Fallback"}
        assert GocApiClient.extract_title(dataset) == "Fallback"


class TestExtractLanguage:
    def test_bilingual_list(self):
        resource = {"language": ["en", "fr"]}
        assert GocApiClient.extract_language(resource) == "bilingual"

    def test_single_language_list(self):
        resource = {"language": ["en"]}
        assert GocApiClient.extract_language(resource) == "en"

    def test_empty_list(self):
        resource = {"language": []}
        assert GocApiClient.extract_language(resource) == "unknown"

    def test_string_language(self):
        resource = {"language": "fr"}
        assert GocApiClient.extract_language(resource) == "fr"

    def test_no_language(self):
        resource = {}
        assert GocApiClient.extract_language(resource) == "unknown"

    def test_none_language(self):
        resource = {"language": None}
        assert GocApiClient.extract_language(resource) == "unknown"


class TestExtractFormat:
    def test_xlsx(self):
        resource = {"format": "XLSX"}
        assert GocApiClient.extract_format(resource) == "xlsx"

    def test_csv(self):
        resource = {"format": "CSV"}
        assert GocApiClient.extract_format(resource) == "csv"

    def test_pdf(self):
        resource = {"format": "PDF"}
        assert GocApiClient.extract_format(resource) == "pdf"

    def test_lowercase_input(self):
        resource = {"format": "csv"}
        assert GocApiClient.extract_format(resource) == "csv"

    def test_with_whitespace(self):
        resource = {"format": "  XLS  "}
        assert GocApiClient.extract_format(resource) == "xls"

    def test_unknown_format(self):
        resource = {"format": "PARQUET"}
        assert GocApiClient.extract_format(resource) == "parquet"

    def test_empty_format(self):
        resource = {"format": ""}
        assert GocApiClient.extract_format(resource) == ""

    def test_no_format_key(self):
        resource = {}
        assert GocApiClient.extract_format(resource) == ""
