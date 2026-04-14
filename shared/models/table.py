from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class ExtractedTable:
    table_id: str
    document_id: str
    table_index: int
    extraction_method: str
    parser_version: str
    headers: list[str]
    rows: list[list[str | None]]
    page_number: int | None = None
    sheet_name: str | None = None
    table_title_raw: str | None = None
    table_subtitle_raw: str | None = None
    section_title_raw: str | None = None
    extraction_confidence: float = 1.0
    gcs_uri: str | None = None

    def to_bq_row(self) -> dict:
        return {
            "table_id": self.table_id,
            "document_id": self.document_id,
            "table_index": self.table_index,
            "page_number": self.page_number,
            "sheet_name": self.sheet_name,
            "table_title_raw": self.table_title_raw,
            "table_subtitle_raw": self.table_subtitle_raw,
            "section_title_raw": self.section_title_raw,
            "extraction_method": self.extraction_method,
            "parser_version": self.parser_version,
            "extraction_confidence": self.extraction_confidence,
            "gcs_uri": self.gcs_uri,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class HeaderRecord:
    header_id: str
    department_id: str
    document_id: str
    table_id: str
    header_raw: str
    header_normalized: str
    header_language: str | None = None
    header_class: str | None = None
    classification_confidence: float | None = None
    first_seen_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_bq_row(self) -> dict:
        return {
            "header_id": self.header_id,
            "department_id": self.department_id,
            "document_id": self.document_id,
            "table_id": self.table_id,
            "header_raw": self.header_raw,
            "header_normalized": self.header_normalized,
            "header_language": self.header_language,
            "header_class": self.header_class,
            "classification_confidence": self.classification_confidence,
            "first_seen_at": self.first_seen_at,
        }


@dataclass
class RowValueLong:
    staging_value_id: str
    department_id: str
    document_id: str
    table_id: str
    source_row_number: int
    source_column_number: int
    header_id: str
    header_raw: str
    row_label_raw: str | None = None
    value_raw: str | None = None
    value_numeric_guess: float | None = None
    value_date_guess: str | None = None
    unit_raw: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_bq_row(self) -> dict:
        return {
            "staging_value_id": self.staging_value_id,
            "department_id": self.department_id,
            "document_id": self.document_id,
            "table_id": self.table_id,
            "source_row_number": self.source_row_number,
            "source_column_number": self.source_column_number,
            "row_label_raw": self.row_label_raw,
            "header_id": self.header_id,
            "header_raw": self.header_raw,
            "value_raw": self.value_raw,
            "value_numeric_guess": self.value_numeric_guess,
            "value_date_guess": self.value_date_guess,
            "unit_raw": self.unit_raw,
            "created_at": self.created_at,
        }
