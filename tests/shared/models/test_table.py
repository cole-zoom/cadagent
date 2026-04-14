"""Tests for shared/models/table.py."""

import pytest

from shared.models.table import ExtractedTable, HeaderRecord, RowValueLong


class TestExtractedTable:
    def test_required_fields(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="d1",
            table_index=0,
            extraction_method="csv",
            parser_version="1.0",
            headers=["Year", "Value"],
            rows=[["2023", "100"]],
        )
        assert table.table_id == "t1"
        assert table.document_id == "d1"
        assert table.table_index == 0

    def test_optional_fields_default_none(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="d1",
            table_index=0,
            extraction_method="csv",
            parser_version="1.0",
            headers=[],
            rows=[],
        )
        assert table.page_number is None
        assert table.sheet_name is None
        assert table.table_title_raw is None
        assert table.table_subtitle_raw is None
        assert table.section_title_raw is None
        assert table.gcs_uri is None

    def test_default_extraction_confidence(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="d1",
            table_index=0,
            extraction_method="csv",
            parser_version="1.0",
            headers=[],
            rows=[],
        )
        assert table.extraction_confidence == 1.0

    def test_to_bq_row_keys(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="d1",
            table_index=0,
            extraction_method="csv",
            parser_version="1.0",
            headers=["A"],
            rows=[["1"]],
        )
        row = table.to_bq_row()
        expected_keys = {
            "table_id", "document_id", "table_index", "page_number",
            "sheet_name", "table_title_raw", "table_subtitle_raw",
            "section_title_raw", "extraction_method", "parser_version",
            "extraction_confidence", "gcs_uri", "created_at",
        }
        assert set(row.keys()) == expected_keys

    def test_to_bq_row_created_at_present(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="d1",
            table_index=0,
            extraction_method="csv",
            parser_version="1.0",
            headers=[],
            rows=[],
        )
        row = table.to_bq_row()
        assert "created_at" in row
        assert row["created_at"] is not None


class TestHeaderRecord:
    def test_required_fields(self):
        header = HeaderRecord(
            header_id="h1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            header_raw="Year",
            header_normalized="year",
        )
        assert header.header_id == "h1"
        assert header.header_raw == "Year"
        assert header.header_normalized == "year"

    def test_optional_fields_default_none(self):
        header = HeaderRecord(
            header_id="h1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            header_raw="Year",
            header_normalized="year",
        )
        assert header.header_language is None
        assert header.header_class is None
        assert header.classification_confidence is None

    def test_first_seen_at_auto_set(self):
        header = HeaderRecord(
            header_id="h1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            header_raw="Year",
            header_normalized="year",
        )
        assert header.first_seen_at is not None
        assert len(header.first_seen_at) > 0

    def test_to_bq_row_keys(self):
        header = HeaderRecord(
            header_id="h1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            header_raw="Year",
            header_normalized="year",
        )
        row = header.to_bq_row()
        expected_keys = {
            "header_id", "department_id", "document_id", "table_id",
            "header_raw", "header_normalized", "header_language",
            "header_class", "classification_confidence", "first_seen_at",
        }
        assert set(row.keys()) == expected_keys


class TestRowValueLong:
    def test_required_fields(self):
        rv = RowValueLong(
            staging_value_id="sv1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            source_row_number=1,
            source_column_number=2,
            header_id="h1",
            header_raw="Year",
        )
        assert rv.staging_value_id == "sv1"
        assert rv.source_row_number == 1
        assert rv.source_column_number == 2

    def test_optional_fields_default_none(self):
        rv = RowValueLong(
            staging_value_id="sv1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            source_row_number=1,
            source_column_number=2,
            header_id="h1",
            header_raw="Year",
        )
        assert rv.row_label_raw is None
        assert rv.value_raw is None
        assert rv.value_numeric_guess is None
        assert rv.value_date_guess is None
        assert rv.unit_raw is None

    def test_created_at_auto_set(self):
        rv = RowValueLong(
            staging_value_id="sv1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            source_row_number=1,
            source_column_number=2,
            header_id="h1",
            header_raw="Year",
        )
        assert rv.created_at is not None
        assert len(rv.created_at) > 0

    def test_to_bq_row_keys(self):
        rv = RowValueLong(
            staging_value_id="sv1",
            department_id="dept1",
            document_id="d1",
            table_id="t1",
            source_row_number=1,
            source_column_number=2,
            header_id="h1",
            header_raw="Year",
        )
        row = rv.to_bq_row()
        expected_keys = {
            "staging_value_id", "department_id", "document_id", "table_id",
            "source_row_number", "source_column_number", "row_label_raw",
            "header_id", "header_raw", "value_raw", "value_numeric_guess",
            "value_date_guess", "unit_raw", "created_at",
        }
        assert set(row.keys()) == expected_keys
