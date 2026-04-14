"""Tests for services/extract/parsers/csv.py."""

from services.extract.parsers.csv import parse_csv, _decode


class TestDecode:
    def test_utf8_decode(self):
        result = _decode(b"hello world")
        assert result == "hello world"

    def test_utf8_bom_decode(self):
        result = _decode(b"\xef\xbb\xbfhello")
        assert result == "hello"

    def test_latin1_decode(self):
        result = _decode(b"M\xe9trique")
        assert result == "M\xe9trique"

    def test_empty_bytes(self):
        result = _decode(b"")
        assert result == ""


class TestParseCsv:
    def test_basic_csv(self):
        data = b"Year,GDP,CPI\n2023,1.5,3.2\n2024,2.0,2.8\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        table = tables[0]
        assert table.headers == ["Year", "GDP", "CPI"]
        assert len(table.rows) == 2
        assert table.rows[0] == ["2023", "1.5", "3.2"]
        assert table.rows[1] == ["2024", "2.0", "2.8"]
        assert table.document_id == "doc1"
        assert table.extraction_method == "csv_parser"

    def test_empty_data(self):
        tables = parse_csv(b"", "doc1")
        assert tables == []

    def test_single_row_header_only(self):
        data = b"A,B,C\n"
        tables = parse_csv(data, "doc1")
        assert tables == []

    def test_trailing_empty_columns_stripped(self):
        data = b"A,B,,\n1,2,,\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        assert tables[0].headers == ["A", "B"]
        assert tables[0].rows[0] == ["1", "2"]

    def test_row_padding(self):
        data = b"A,B,C\n1\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        assert tables[0].rows[0] == ["1", None, None]

    def test_latin1_encoding(self):
        data = b"M\xe9trique,Valeur\nTest,123\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        assert tables[0].headers[0] == "M\xe9trique"
        assert tables[0].rows[0] == ["Test", "123"]

    def test_empty_rows_filtered(self):
        data = b"A,B\n1,2\n,,\n3,4\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        assert len(tables[0].rows) == 2
        assert tables[0].rows[0] == ["1", "2"]
        assert tables[0].rows[1] == ["3", "4"]

    def test_resource_name_stored_as_title(self):
        data = b"A,B\n1,2\n"
        tables = parse_csv(data, "doc1", resource_name="my_file")
        assert tables[0].table_title_raw == "my_file"

    def test_table_index_offset(self):
        data = b"A,B\n1,2\n"
        tables = parse_csv(data, "doc1", table_index_offset=5)
        assert tables[0].table_index == 5

    def test_table_id_is_deterministic(self):
        data = b"A,B\n1,2\n"
        t1 = parse_csv(data, "doc1")
        t2 = parse_csv(data, "doc1")
        assert t1[0].table_id == t2[0].table_id

    def test_row_truncated_to_header_length(self):
        data = b"A,B\n1,2,3,4\n"
        tables = parse_csv(data, "doc1")
        assert len(tables) == 1
        assert tables[0].rows[0] == ["1", "2"]
