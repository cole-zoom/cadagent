from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class DocumentRecord:
    document_id: str
    department_id: str
    department_code: str
    gcs_uri: str
    source_url: str
    title: str
    document_type: str
    file_format: str
    language: str
    published_date: str | None = None
    effective_date: str | None = None
    fiscal_year_label: str | None = None
    checksum: str = ""
    source_system: str = "open.canada.ca"
    ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ingestion_status: str = "success"

    def to_bq_row(self) -> dict:
        return {
            "document_id": self.document_id,
            "department_id": self.department_id,
            "department_code": self.department_code,
            "gcs_uri": self.gcs_uri,
            "source_url": self.source_url,
            "title": self.title,
            "document_type": self.document_type,
            "file_format": self.file_format,
            "language": self.language,
            "published_date": self.published_date,
            "effective_date": self.effective_date,
            "fiscal_year_label": self.fiscal_year_label,
            "checksum": self.checksum,
            "source_system": self.source_system,
            "ingested_at": self.ingested_at,
            "ingestion_status": self.ingestion_status,
        }
