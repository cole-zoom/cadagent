"""Tests for services/extract/parsers/xlsx.py."""

import sys
from unittest.mock import MagicMock, patch

from services.extract.parsers.xlsx import parse_xlsx


class TestParseXlsx:
    def test_basic_xlsx(self):
        mock_openpyxl = MagicMock()
        mock_ws = MagicMock()
        mock_ws.iter_rows.return_value = [
            ("Year", "GDP", "CPI"),
            (2023, 1.5, 3.2),
            (2024, 2.0, 2.8),
        ]

        mock_wb = MagicMock()
        mock_wb.sheetnames = ["Sheet1"]
        mock_wb.__getitem__ = MagicMock(return_value=mock_ws)
        mock_openpyxl.load_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"openpyxl": mock_openpyxl}):
            tables = parse_xlsx(b"fake xlsx data", "doc1", file_format="xlsx")

        assert len(tables) == 1
        assert tables[0].headers == ["Year", "GDP", "CPI"]
        assert len(tables[0].rows) == 2
        assert tables[0].extraction_method == "xlsx_parser"
        assert tables[0].sheet_name == "Sheet1"

    def test_corrupted_file(self):
        mock_openpyxl = MagicMock()
        mock_openpyxl.load_workbook.side_effect = Exception("corrupted")

        with patch.dict(sys.modules, {"openpyxl": mock_openpyxl}):
            tables = parse_xlsx(b"bad data", "doc1", file_format="xlsx")

        assert tables == []

    def test_empty_sheet(self):
        mock_openpyxl = MagicMock()
        mock_ws = MagicMock()
        mock_ws.iter_rows.return_value = [("Header",)]  # only 1 row

        mock_wb = MagicMock()
        mock_wb.sheetnames = ["Sheet1"]
        mock_wb.__getitem__ = MagicMock(return_value=mock_ws)
        mock_openpyxl.load_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"openpyxl": mock_openpyxl}):
            tables = parse_xlsx(b"fake data", "doc1", file_format="xlsx")

        assert tables == []

    def test_multiple_sheets(self):
        mock_openpyxl = MagicMock()
        mock_ws1 = MagicMock()
        mock_ws1.iter_rows.return_value = [
            ("A", "B"),
            (1, 2),
        ]
        mock_ws2 = MagicMock()
        mock_ws2.iter_rows.return_value = [
            ("C", "D"),
            (3, 4),
        ]

        mock_wb = MagicMock()
        mock_wb.sheetnames = ["Sheet1", "Sheet2"]
        mock_wb.__getitem__ = MagicMock(side_effect=lambda name: {
            "Sheet1": mock_ws1,
            "Sheet2": mock_ws2,
        }[name])
        mock_openpyxl.load_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"openpyxl": mock_openpyxl}):
            tables = parse_xlsx(b"fake data", "doc1", file_format="xlsx")

        assert len(tables) == 2
        assert tables[0].sheet_name == "Sheet1"
        assert tables[1].sheet_name == "Sheet2"

    def test_empty_rows_filtered(self):
        mock_openpyxl = MagicMock()
        mock_ws = MagicMock()
        mock_ws.iter_rows.return_value = [
            ("A", "B"),
            (1, 2),
            (None, None),
            (3, 4),
        ]

        mock_wb = MagicMock()
        mock_wb.sheetnames = ["Sheet1"]
        mock_wb.__getitem__ = MagicMock(return_value=mock_ws)
        mock_openpyxl.load_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"openpyxl": mock_openpyxl}):
            tables = parse_xlsx(b"fake data", "doc1", file_format="xlsx")

        assert len(tables) == 1
        assert len(tables[0].rows) == 2


class TestParseXls:
    def test_basic_xls(self):
        mock_xlrd = MagicMock()
        mock_ws = MagicMock()
        mock_ws.nrows = 3
        mock_ws.ncols = 2
        mock_ws.name = "Sheet1"

        def cell_value(r, c):
            data = [
                ["Year", "GDP"],
                ["2023", "1.5"],
                ["2024", "2.0"],
            ]
            return data[r][c]

        mock_ws.cell_value = cell_value

        mock_wb = MagicMock()
        mock_wb.nsheets = 1
        mock_wb.sheet_by_index.return_value = mock_ws
        mock_xlrd.open_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"xlrd": mock_xlrd}):
            tables = parse_xlsx(b"fake xls data", "doc1", file_format="xls")

        assert len(tables) == 1
        assert tables[0].headers == ["Year", "GDP"]
        assert len(tables[0].rows) == 2
        assert tables[0].extraction_method == "xls_parser"
        assert tables[0].sheet_name == "Sheet1"

    def test_xls_corrupted(self):
        mock_xlrd = MagicMock()
        mock_xlrd.open_workbook.side_effect = Exception("corrupted")

        with patch.dict(sys.modules, {"xlrd": mock_xlrd}):
            tables = parse_xlsx(b"bad data", "doc1", file_format="xls")

        assert tables == []

    def test_xls_empty_sheet(self):
        mock_xlrd = MagicMock()
        mock_ws = MagicMock()
        mock_ws.nrows = 1  # only header row
        mock_ws.ncols = 2
        mock_ws.name = "Sheet1"

        mock_wb = MagicMock()
        mock_wb.nsheets = 1
        mock_wb.sheet_by_index.return_value = mock_ws
        mock_xlrd.open_workbook.return_value = mock_wb

        with patch.dict(sys.modules, {"xlrd": mock_xlrd}):
            tables = parse_xlsx(b"fake data", "doc1", file_format="xls")

        assert tables == []
