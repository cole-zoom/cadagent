"""Tests for shared/clients/gcs.py."""

from unittest.mock import MagicMock, patch

import pytest

from shared.clients.gcs import GcsClient


@pytest.fixture
def mock_storage_client():
    with patch("shared.clients.gcs.storage.Client") as mock_cls:
        mock_client_instance = MagicMock()
        mock_cls.return_value = mock_client_instance
        yield mock_client_instance


@pytest.fixture
def gcs_client(mock_storage_client):
    return GcsClient(project_id="test-project")


class TestParseUri:
    def test_valid_uri(self):
        bucket, path = GcsClient._parse_uri("gs://my-bucket/path/to/file.csv")
        assert bucket == "my-bucket"
        assert path == "path/to/file.csv"

    def test_valid_uri_no_path(self):
        bucket, path = GcsClient._parse_uri("gs://my-bucket/")
        assert bucket == "my-bucket"
        assert path == ""

    def test_valid_uri_bucket_only(self):
        bucket, path = GcsClient._parse_uri("gs://my-bucket")
        assert bucket == "my-bucket"

    def test_invalid_uri_raises(self):
        with pytest.raises(ValueError, match="Invalid GCS URI"):
            GcsClient._parse_uri("https://storage.googleapis.com/bucket/path")

    def test_invalid_uri_no_prefix(self):
        with pytest.raises(ValueError, match="Invalid GCS URI"):
            GcsClient._parse_uri("my-bucket/path")


class TestUploadRawFile:
    def test_returns_correct_uri(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        uri = gcs_client.upload_raw_file(
            bucket_name="test-bucket",
            department="fin",
            year="2024",
            document_id="abc123",
            filename="data.csv",
            data=b"test data",
        )
        expected = "gs://test-bucket/raw/goc/department=fin/year=2024/document_id=abc123/data.csv"
        assert uri == expected

    def test_calls_upload_from_string(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        gcs_client.upload_raw_file(
            bucket_name="test-bucket",
            department="fin",
            year="2024",
            document_id="abc123",
            filename="data.csv",
            data=b"test data",
        )
        mock_blob.upload_from_string.assert_called_once_with(b"test data")

    def test_constructs_correct_path(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        gcs_client.upload_raw_file(
            bucket_name="test-bucket",
            department="statcan",
            year="2023",
            document_id="xyz789",
            filename="report.xlsx",
            data=b"content",
        )
        expected_path = "raw/goc/department=statcan/year=2023/document_id=xyz789/report.xlsx"
        mock_bucket.blob.assert_called_once_with(expected_path)


class TestFileExists:
    def test_returns_true_when_exists(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        assert gcs_client.file_exists("gs://test-bucket/path/to/file.csv") is True

    def test_returns_false_when_not_exists(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        assert gcs_client.file_exists("gs://test-bucket/path/to/file.csv") is False


class TestDownloadFile:
    def test_returns_bytes(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"file contents"
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        result = gcs_client.download_file("gs://test-bucket/path/to/file.csv")
        assert result == b"file contents"

    def test_calls_download_as_bytes(self, gcs_client, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b""
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        gcs_client.download_file("gs://test-bucket/my/path.csv")
        mock_blob.download_as_bytes.assert_called_once()
