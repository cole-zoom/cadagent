"""Tests for services/extract/run.py."""

from unittest.mock import MagicMock, patch

import pytest

from shared.models.table import ExtractedTable
from services.extract.config import ExtractConfig
from services.extract.run import (
    _route_to_parser,
    _try_parse_numeric,
    _build_header_records,
    extract_document,
)


class TestTryParseNumeric:
    def test_integer_with_commas(self):
        assert _try_parse_numeric("1,234.56") == pytest.approx(1234.56)

    def test_dollar_sign(self):
        assert _try_parse_numeric("$100") == pytest.approx(100.0)

    def test_percent_sign(self):
        assert _try_parse_numeric("3.5%") == pytest.approx(3.5)

    def test_dash_returns_none(self):
        assert _try_parse_numeric("-") is None

    def test_ellipsis_returns_none(self):
        assert _try_parse_numeric("...") is None

    def test_x_returns_none(self):
        assert _try_parse_numeric("x") is None

    def test_na_returns_none(self):
        assert _try_parse_numeric("n/a") is None

    def test_na_no_slash_returns_none(self):
        assert _try_parse_numeric("na") is None

    def test_none_returns_none(self):
        assert _try_parse_numeric(None) is None

    def test_empty_string_returns_none(self):
        assert _try_parse_numeric("") is None

    def test_f_returns_none(self):
        assert _try_parse_numeric("F") is None

    def test_e_returns_none(self):
        assert _try_parse_numeric("E") is None

    def test_plain_float(self):
        assert _try_parse_numeric("3.14") == pytest.approx(3.14)

    def test_negative_number(self):
        assert _try_parse_numeric("-42.5") == pytest.approx(-42.5)

    def test_non_numeric_string(self):
        assert _try_parse_numeric("hello") is None

    def test_spaces_removed(self):
        assert _try_parse_numeric(" 1 234 ") == pytest.approx(1234.0)


class TestRouteToParser:
    def test_csv_format(self):
        data = b"Year,GDP\n2023,1.5\n2024,2.0\n"
        tables = _route_to_parser(data, "doc1", "csv", "test")
        assert len(tables) == 1
        assert tables[0].extraction_method == "csv_parser"

    def test_csv_format_uppercase(self):
        data = b"Year,GDP\n2023,1.5\n2024,2.0\n"
        tables = _route_to_parser(data, "doc1", "CSV", "test")
        assert len(tables) == 1

    def test_html_format(self):
        data = b"""<html><body>
<table><tr><th>A</th></tr><tr><td>1</td></tr></table>
</body></html>"""
        tables = _route_to_parser(data, "doc1", "html", "test")
        assert len(tables) == 1
        assert tables[0].extraction_method == "html_parser"

    def test_xml_format(self):
        data = b"""<?xml version="1.0"?>
<root><item name="a"><val>1</val></item><item name="b"><val>2</val></item></root>"""
        tables = _route_to_parser(data, "doc1", "xml", "test")
        assert len(tables) == 1

    def test_unknown_format_returns_empty(self):
        tables = _route_to_parser(b"data", "doc1", "pdf", "test")
        assert tables == []

    @patch("services.extract.run.parse_xlsx")
    def test_xlsx_format_routes_to_xlsx_parser(self, mock_parse_xlsx):
        mock_parse_xlsx.return_value = []
        _route_to_parser(b"data", "doc1", "xlsx", "test")
        mock_parse_xlsx.assert_called_once_with(b"data", "doc1", file_format="xlsx")

    @patch("services.extract.run.parse_xlsx")
    def test_xls_format_routes_to_xlsx_parser(self, mock_parse_xlsx):
        mock_parse_xlsx.return_value = []
        _route_to_parser(b"data", "doc1", "xls", "test")
        mock_parse_xlsx.assert_called_once_with(b"data", "doc1", file_format="xls")


class TestBuildHeaderRecords:
    def test_correct_count(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="doc1",
            table_index=0,
            extraction_method="csv_parser",
            parser_version="0.1.0",
            headers=["Year", "GDP", "CPI"],
            rows=[["2023", "1.5", "3.2"]],
        )
        records = _build_header_records(table, "fin", "doc1")
        assert len(records) == 3

    def test_header_fields(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="doc1",
            table_index=0,
            extraction_method="csv_parser",
            parser_version="0.1.0",
            headers=["Year"],
            rows=[["2023"]],
        )
        records = _build_header_records(table, "fin", "doc1")
        assert len(records) == 1
        rec = records[0]
        assert rec.department_id == "fin"
        assert rec.document_id == "doc1"
        assert rec.table_id == "t1"
        assert rec.header_raw == "Year"
        assert rec.header_id  # non-empty string
        assert rec.header_normalized  # non-empty string

    def test_empty_headers(self):
        table = ExtractedTable(
            table_id="t1",
            document_id="doc1",
            table_index=0,
            extraction_method="csv_parser",
            parser_version="0.1.0",
            headers=[],
            rows=[],
        )
        records = _build_header_records(table, "fin", "doc1")
        assert records == []


class TestExtractDocument:
    def test_success_path(self):
        gcs_client = MagicMock()
        bq_client = MagicMock()
        config = ExtractConfig()

        gcs_client.download_file.return_value = b"Year,GDP\n2023,1.5\n2024,2.0\n"
        bq_client.insert_rows.return_value = []

        stats = extract_document(
            document_id="doc1",
            department_id="fin",
            file_format="csv",
            gcs_uri="gs://bucket/file.csv",
            title="Test CSV",
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="raw-bucket",
            processed_bucket="processed-bucket",
            raw_dataset="raw_ds",
            stg_dataset="stg_ds",
        )

        assert stats["tables"] == 1
        assert stats["headers"] > 0
        assert stats["rows"] > 0
        # Should have been called for extracted_tables, headers, and row_values_long
        assert bq_client.insert_rows.call_count >= 3

    def test_download_failure(self):
        gcs_client = MagicMock()
        bq_client = MagicMock()
        config = ExtractConfig()

        gcs_client.download_file.side_effect = Exception("download error")

        stats = extract_document(
            document_id="doc1",
            department_id="fin",
            file_format="csv",
            gcs_uri="gs://bucket/file.csv",
            title="Test CSV",
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="raw-bucket",
            processed_bucket="processed-bucket",
            raw_dataset="raw_ds",
            stg_dataset="stg_ds",
        )

        assert stats["tables"] == 0
        assert "error" in stats
        bq_client.insert_rows.assert_not_called()

    def test_max_tables_truncation(self):
        gcs_client = MagicMock()
        bq_client = MagicMock()
        config = ExtractConfig(max_tables_per_document=1)

        # HTML with 2 tables, each with enough rows to pass min_rows_for_table
        gcs_client.download_file.return_value = b"""<html><body>
<table>
  <tr><th>A</th></tr>
  <tr><td>1</td></tr>
  <tr><td>2</td></tr>
</table>
<table>
  <tr><th>B</th></tr>
  <tr><td>3</td></tr>
  <tr><td>4</td></tr>
</table>
</body></html>"""
        bq_client.insert_rows.return_value = []

        stats = extract_document(
            document_id="doc1",
            department_id="fin",
            file_format="html",
            gcs_uri="gs://bucket/file.html",
            title="Test HTML",
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="raw-bucket",
            processed_bucket="processed-bucket",
            raw_dataset="raw_ds",
            stg_dataset="stg_ds",
        )

        # Truncated to 1 table
        assert stats["tables"] == 1

    def test_no_parser_for_format(self):
        gcs_client = MagicMock()
        bq_client = MagicMock()
        config = ExtractConfig()

        gcs_client.download_file.return_value = b"some binary data"
        bq_client.insert_rows.return_value = []

        stats = extract_document(
            document_id="doc1",
            department_id="fin",
            file_format="pdf",
            gcs_uri="gs://bucket/file.pdf",
            title="Test PDF",
            gcs_client=gcs_client,
            bq_client=bq_client,
            config=config,
            raw_bucket="raw-bucket",
            processed_bucket="processed-bucket",
            raw_dataset="raw_ds",
            stg_dataset="stg_ds",
        )

        assert stats["tables"] == 0
        assert stats["headers"] == 0
        assert stats["rows"] == 0
