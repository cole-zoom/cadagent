"""Tests for services/extract/parsers/xml.py."""

from services.extract.parsers.xml import parse_xml, _local_name


class TestLocalName:
    def test_with_namespace(self):
        assert _local_name("{http://ns}Tag") == "Tag"

    def test_without_namespace(self):
        assert _local_name("Tag") == "Tag"

    def test_empty_namespace(self):
        assert _local_name("{}Tag") == "Tag"

    def test_nested_braces(self):
        assert _local_name("{http://example.com/ns}Element") == "Element"


class TestParseSdmxData:
    def test_basic_sdmx_data(self):
        xml_data = b"""<?xml version="1.0"?>
<DataSet>
  <Series Geography="Canada">
    <Obs TIME_PERIOD="2023" OBS_VALUE="100"/>
    <Obs TIME_PERIOD="2024" OBS_VALUE="105"/>
  </Series>
</DataSet>"""
        tables = parse_xml(xml_data, "doc1")
        assert len(tables) == 1
        table = tables[0]
        assert table.extraction_method == "sdmx_data_parser"
        assert "Geography" in table.headers
        assert "TIME_PERIOD" in table.headers
        assert "OBS_VALUE" in table.headers
        assert len(table.rows) == 2

    def test_sdmx_data_row_values(self):
        xml_data = b"""<?xml version="1.0"?>
<DataSet>
  <Series Geography="Canada">
    <Obs TIME_PERIOD="2023" OBS_VALUE="100"/>
  </Series>
</DataSet>"""
        tables = parse_xml(xml_data, "doc1")
        table = tables[0]
        headers = table.headers
        row = table.rows[0]
        row_dict = dict(zip(headers, row))
        assert row_dict["Geography"] == "Canada"
        assert row_dict["TIME_PERIOD"] == "2023"
        assert row_dict["OBS_VALUE"] == "100"

    def test_sdmx_multiple_series(self):
        xml_data = b"""<?xml version="1.0"?>
<DataSet>
  <Series Geography="Canada">
    <Obs TIME_PERIOD="2023" OBS_VALUE="100"/>
  </Series>
  <Series Geography="Ontario">
    <Obs TIME_PERIOD="2023" OBS_VALUE="50"/>
  </Series>
</DataSet>"""
        tables = parse_xml(xml_data, "doc1")
        assert len(tables) == 1
        assert len(tables[0].rows) == 2


class TestParseSdmxStructure:
    def test_basic_structure(self):
        xml_data = b"""<?xml version="1.0"?>
<Structure>
  <Codelist id="CL_GEO">
    <Code id="CA"><Name>Canada</Name></Code>
    <Code id="ON"><Name>Ontario</Name></Code>
  </Codelist>
</Structure>"""
        tables = parse_xml(xml_data, "doc1")
        assert len(tables) == 1
        table = tables[0]
        assert table.extraction_method == "sdmx_structure_parser"
        assert "id" in table.headers
        assert "Name" in table.headers
        assert len(table.rows) == 2
        assert table.table_title_raw == "Codelist: CL_GEO"

    def test_structure_row_values(self):
        xml_data = b"""<?xml version="1.0"?>
<Structure>
  <Codelist id="CL_GEO">
    <Code id="CA"><Name>Canada</Name></Code>
  </Codelist>
</Structure>"""
        tables = parse_xml(xml_data, "doc1")
        table = tables[0]
        headers = table.headers
        row = table.rows[0]
        row_dict = dict(zip(headers, row))
        assert row_dict["id"] == "CA"
        assert row_dict["Name"] == "Canada"


class TestSpreadsheetML:
    def test_spreadsheetml_skipped(self):
        xml_data = b"""<?xml version="1.0"?>
<Workbook><Worksheet><Table><Row><Cell>data</Cell></Row></Table></Worksheet></Workbook>"""
        tables = parse_xml(xml_data, "doc1")
        assert tables == []


class TestGenericXml:
    def test_generic_xml_repeated_children(self):
        xml_data = b"""<?xml version="1.0"?>
<root>
  <item name="a"><value>1</value></item>
  <item name="b"><value>2</value></item>
  <item name="c"><value>3</value></item>
</root>"""
        tables = parse_xml(xml_data, "doc1")
        assert len(tables) == 1
        table = tables[0]
        assert table.extraction_method == "generic_xml_parser"
        assert "name" in table.headers
        assert "value" in table.headers
        assert len(table.rows) == 3

    def test_generic_xml_single_child_no_table(self):
        xml_data = b"""<?xml version="1.0"?>
<root>
  <item name="a"><value>1</value></item>
</root>"""
        tables = parse_xml(xml_data, "doc1")
        assert tables == []


class TestInvalidXml:
    def test_invalid_xml(self):
        tables = parse_xml(b"not xml", "doc1")
        assert tables == []

    def test_empty_bytes(self):
        tables = parse_xml(b"", "doc1")
        assert tables == []
