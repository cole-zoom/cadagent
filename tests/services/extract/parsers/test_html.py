"""Tests for services/extract/parsers/html.py."""

from services.extract.parsers.html import parse_html


class TestParseHtml:
    def test_basic_table(self):
        html = b"""<html><body>
<table>
  <tr><th>Province</th><th>Population</th></tr>
  <tr><td>Ontario</td><td>15000000</td></tr>
  <tr><td>Quebec</td><td>8700000</td></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1")
        assert len(tables) == 1
        table = tables[0]
        assert table.headers == ["Province", "Population"]
        assert len(table.rows) == 2
        assert table.rows[0] == ["Ontario", "15000000"]
        assert table.rows[1] == ["Quebec", "8700000"]
        assert table.extraction_method == "html_parser"

    def test_multiple_tables(self):
        html = b"""<html><body>
<table>
  <tr><th>A</th><th>B</th></tr>
  <tr><td>1</td><td>2</td></tr>
</table>
<table>
  <tr><th>C</th><th>D</th></tr>
  <tr><td>3</td><td>4</td></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1")
        assert len(tables) == 2
        assert tables[0].headers == ["A", "B"]
        assert tables[1].headers == ["C", "D"]
        assert tables[0].table_index == 0
        assert tables[1].table_index == 1

    def test_no_tables(self):
        html = b"<html><body><p>No tables</p></body></html>"
        tables = parse_html(html, "doc1")
        assert tables == []

    def test_uneven_rows_padded(self):
        html = b"""<html><body>
<table>
  <tr><th>A</th><th>B</th></tr>
  <tr><td>1</td><td>2</td><td>3</td></tr>
  <tr><td>4</td></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1")
        assert len(tables) == 1
        table = tables[0]
        # max_cols is max(2 headers, 3 first row, 1 second row) = 3
        assert len(table.headers) == 3
        assert table.headers == ["A", "B", ""]
        assert table.rows[0] == ["1", "2", "3"]
        assert table.rows[1] == ["4", None, None]

    def test_single_row_table_skipped(self):
        html = b"""<html><body>
<table>
  <tr><th>A</th><th>B</th></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1")
        assert tables == []

    def test_resource_name_as_title(self):
        html = b"""<html><body>
<table>
  <tr><th>A</th></tr>
  <tr><td>1</td></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1", resource_name="my_table")
        assert len(tables) == 1
        assert tables[0].table_title_raw == "my_table"

    def test_empty_data_rows_filtered(self):
        html = b"""<html><body>
<table>
  <tr><th>A</th><th>B</th></tr>
  <tr><td>1</td><td>2</td></tr>
  <tr><td></td><td></td></tr>
  <tr><td>3</td><td>4</td></tr>
</table>
</body></html>"""
        tables = parse_html(html, "doc1")
        assert len(tables) == 1
        assert len(tables[0].rows) == 2
