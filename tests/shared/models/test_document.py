"""Tests for shared/models/document.py."""

import pytest

from shared.models.document import DocumentRecord


class TestDocumentRecord:
    def test_required_fields(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test Document",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.document_id == "doc1"
        assert doc.department_id == "dept1"
        assert doc.department_code == "fin"

    def test_default_source_system(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.source_system == "open.canada.ca"

    def test_default_ingestion_status(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.ingestion_status == "success"

    def test_ingested_at_auto_set(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.ingested_at is not None
        assert len(doc.ingested_at) > 0

    def test_optional_fields_default_none(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.published_date is None
        assert doc.effective_date is None
        assert doc.fiscal_year_label is None

    def test_default_checksum_empty(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        assert doc.checksum == ""

    def test_to_bq_row_has_all_fields(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test Document",
            document_type="budget",
            file_format="csv",
            language="en",
        )
        row = doc.to_bq_row()
        expected_keys = {
            "document_id", "department_id", "department_code", "gcs_uri",
            "source_url", "title", "document_type", "file_format", "language",
            "published_date", "effective_date", "fiscal_year_label",
            "checksum", "source_system", "ingested_at", "ingestion_status",
        }
        assert set(row.keys()) == expected_keys

    def test_to_bq_row_values(self):
        doc = DocumentRecord(
            document_id="doc1",
            department_id="dept1",
            department_code="fin",
            gcs_uri="gs://bucket/path",
            source_url="https://example.com/file.csv",
            title="Test",
            document_type="budget",
            file_format="csv",
            language="en",
            published_date="2024-01-01",
        )
        row = doc.to_bq_row()
        assert row["document_id"] == "doc1"
        assert row["published_date"] == "2024-01-01"
        assert row["source_system"] == "open.canada.ca"
        assert row["ingestion_status"] == "success"
