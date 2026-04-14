"""XML parser for GoC data files.

Handles:
- StatCan SDMX data XML (@TIME_PERIOD, @OBS_VALUE observations)
- StatCan SDMX structure XML (Codelists, Concepts -- captured as metadata tables)
- SpreadsheetML detection and skip (TBS-SCT formatting artifacts)
"""

import hashlib
import logging
from xml.etree import ElementTree as ET

from shared.models.table import ExtractedTable

logger = logging.getLogger(__name__)

PARSER_VERSION = "0.1.0"

SPREADSHEETML_TAGS = {"Workbook", "Worksheet", "Table", "Row", "Cell", "Style", "Font", "Interior"}


def parse_xml(
    data: bytes,
    document_id: str,
    resource_name: str = "",
) -> list[ExtractedTable]:
    """Parse an XML file, routing to SDMX or SpreadsheetML handlers."""
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        logger.warning("Failed to parse XML for %s: %s", document_id, e)
        return []

    # Detect SpreadsheetML (formatting junk from tbs-sct)
    root_tag = _local_name(root.tag)
    if root_tag in SPREADSHEETML_TAGS:
        logger.info("Skipping SpreadsheetML file: %s", document_id)
        return []

    # Detect SDMX by looking for common SDMX elements
    all_tags = {_local_name(elem.tag) for elem in root.iter()}

    if "Series" in all_tags and "Obs" in all_tags:
        return _parse_sdmx_data(root, document_id, resource_name)
    elif "Codelist" in all_tags or "ConceptScheme" in all_tags:
        return _parse_sdmx_structure(root, document_id, resource_name)
    else:
        return _parse_generic_xml(root, document_id, resource_name)


def _parse_sdmx_data(
    root: ET.Element, document_id: str, resource_name: str
) -> list[ExtractedTable]:
    """Parse SDMX data XML: each Series with Obs children becomes rows."""
    tables = []
    table_idx = 0

    for dataset_elem in _find_all_local(root, "DataSet"):
        series_elems = list(_find_all_local(dataset_elem, "Series"))
        if not series_elems:
            continue

        # Collect all possible attribute keys across all series
        all_series_keys: set[str] = set()
        all_obs_keys: set[str] = set()
        for series in series_elems:
            all_series_keys.update(series.attrib.keys())
            for obs in _find_all_local(series, "Obs"):
                all_obs_keys.update(obs.attrib.keys())

        headers = sorted(all_series_keys) + sorted(all_obs_keys - all_series_keys)
        rows = []

        for series in series_elems:
            series_attrs = dict(series.attrib)
            for obs in _find_all_local(series, "Obs"):
                row_dict = {**series_attrs, **dict(obs.attrib)}
                row = [row_dict.get(h) for h in headers]
                rows.append(row)

        if rows:
            table_id = hashlib.sha256(
                f"{document_id}|sdmx_data|{table_idx}".encode()
            ).hexdigest()[:32]

            tables.append(ExtractedTable(
                table_id=table_id,
                document_id=document_id,
                table_index=table_idx,
                extraction_method="sdmx_data_parser",
                parser_version=PARSER_VERSION,
                headers=headers,
                rows=rows,
                table_title_raw=resource_name or None,
            ))
            table_idx += 1

    return tables


def _parse_sdmx_structure(
    root: ET.Element, document_id: str, resource_name: str
) -> list[ExtractedTable]:
    """Parse SDMX structure XML (Codelists, Concepts) as metadata tables."""
    tables = []
    table_idx = 0

    for codelist in _find_all_local(root, "Codelist"):
        codelist_id = codelist.attrib.get("id", f"codelist_{table_idx}")
        codes = list(_find_all_local(codelist, "Code"))

        if not codes:
            continue

        all_keys: set[str] = set()
        for code in codes:
            all_keys.update(code.attrib.keys())
            for child in code:
                all_keys.add(_local_name(child.tag))

        headers = sorted(all_keys)
        rows = []
        for code in codes:
            row_dict = dict(code.attrib)
            for child in code:
                tag = _local_name(child.tag)
                row_dict[tag] = child.text or ""
            rows.append([row_dict.get(h) for h in headers])

        if rows:
            table_id = hashlib.sha256(
                f"{document_id}|sdmx_structure|{table_idx}|{codelist_id}".encode()
            ).hexdigest()[:32]

            tables.append(ExtractedTable(
                table_id=table_id,
                document_id=document_id,
                table_index=table_idx,
                extraction_method="sdmx_structure_parser",
                parser_version=PARSER_VERSION,
                headers=headers,
                rows=rows,
                table_title_raw=f"Codelist: {codelist_id}",
            ))
            table_idx += 1

    return tables


def _parse_generic_xml(
    root: ET.Element, document_id: str, resource_name: str
) -> list[ExtractedTable]:
    """Fallback: treat repeated child elements as rows."""
    child_tags: dict[str, int] = {}
    for child in root:
        tag = _local_name(child.tag)
        child_tags[tag] = child_tags.get(tag, 0) + 1

    if not child_tags:
        return []

    # Use the most common child tag as the row element
    row_tag = max(child_tags, key=lambda t: child_tags[t])
    row_elements = [c for c in root if _local_name(c.tag) == row_tag]

    if len(row_elements) < 2:
        return []

    all_keys: set[str] = set()
    for elem in row_elements:
        all_keys.update(elem.attrib.keys())
        for child in elem:
            all_keys.add(_local_name(child.tag))

    headers = sorted(all_keys)
    rows = []
    for elem in row_elements:
        row_dict = dict(elem.attrib)
        for child in elem:
            row_dict[_local_name(child.tag)] = child.text or ""
        rows.append([row_dict.get(h) for h in headers])

    table_id = hashlib.sha256(
        f"{document_id}|generic_xml|{row_tag}".encode()
    ).hexdigest()[:32]

    return [ExtractedTable(
        table_id=table_id,
        document_id=document_id,
        table_index=0,
        extraction_method="generic_xml_parser",
        parser_version=PARSER_VERSION,
        headers=headers,
        rows=rows,
        table_title_raw=resource_name or None,
    )]


def _local_name(tag: str) -> str:
    """Strip namespace from an XML tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_all_local(elem: ET.Element, local_name: str):
    """Find all descendants matching a local tag name (ignoring namespaces)."""
    for child in elem.iter():
        if _local_name(child.tag) == local_name:
            yield child
