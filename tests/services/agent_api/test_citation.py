"""Tests for services/agent_api/citation.py."""

from unittest.mock import MagicMock

from services.agent_api.citation import Citation, format_citations, lookup_citations


class TestLookupCitations:
    def test_empty_document_ids(self, mock_bq_client):
        result = lookup_citations(mock_bq_client, "proj", "cur", "raw", [])
        assert result == []
        mock_bq_client.query.assert_not_called()

    def test_success(self, mock_bq_client):
        mock_bq_client.query.return_value = [
            {
                "document_id": "doc-1",
                "title": "Budget 2025",
                "source_url": "https://example.com/budget.csv",
                "department_id": "fin",
                "table_title_raw": "Table A1",
                "sheet_name": "Sheet1",
                "page_number": 5,
            }
        ]

        result = lookup_citations(mock_bq_client, "proj", "cur", "raw", ["doc-1"])

        assert len(result) == 1
        assert result[0].document_id == "doc-1"
        assert result[0].title == "Budget 2025"
        assert result[0].source_url == "https://example.com/budget.csv"
        assert result[0].department == "fin"
        assert result[0].table_title == "Table A1"
        assert result[0].sheet_name == "Sheet1"
        assert result[0].page_number == 5

    def test_dedup_by_document_id(self, mock_bq_client):
        """Two rows with the same document_id produce only one Citation."""
        mock_bq_client.query.return_value = [
            {
                "document_id": "doc-1",
                "title": "Budget 2025",
                "source_url": "https://example.com/budget.csv",
                "department_id": "fin",
                "table_title_raw": "Table A1",
                "sheet_name": None,
                "page_number": None,
            },
            {
                "document_id": "doc-1",
                "title": "Budget 2025",
                "source_url": "https://example.com/budget.csv",
                "department_id": "fin",
                "table_title_raw": "Table A2",
                "sheet_name": None,
                "page_number": None,
            },
        ]

        result = lookup_citations(mock_bq_client, "proj", "cur", "raw", ["doc-1"])
        assert len(result) == 1

    def test_query_failure_returns_empty(self, mock_bq_client):
        """If the BQ query throws, return empty list (logged error)."""
        mock_bq_client.query.side_effect = Exception("BQ timeout")
        result = lookup_citations(mock_bq_client, "proj", "cur", "raw", ["doc-1"])
        assert result == []

    def test_multiple_documents(self, mock_bq_client):
        mock_bq_client.query.return_value = [
            {
                "document_id": "doc-1",
                "title": "Budget 2025",
                "source_url": "https://example.com/a.csv",
                "department_id": "fin",
                "table_title_raw": None,
                "sheet_name": None,
                "page_number": None,
            },
            {
                "document_id": "doc-2",
                "title": "Labour Stats",
                "source_url": "https://example.com/b.csv",
                "department_id": "statcan",
                "table_title_raw": None,
                "sheet_name": None,
                "page_number": None,
            },
        ]

        result = lookup_citations(mock_bq_client, "proj", "cur", "raw", ["doc-1", "doc-2"])
        assert len(result) == 2
        doc_ids = {c.document_id for c in result}
        assert doc_ids == {"doc-1", "doc-2"}


class TestFormatCitations:
    def test_format_citations(self):
        citations = [
            Citation(
                document_id="doc-1",
                title="Budget 2025",
                source_url="https://example.com/budget.csv",
                department="fin",
                table_title="Table A1",
            ),
        ]
        result = format_citations(citations)
        assert "Budget 2025" in result
        assert "https://example.com/budget.csv" in result
        assert "fin" in result
        assert "Table A1" in result

    def test_format_citations_empty(self):
        result = format_citations([])
        assert result == "No source documents found."

    def test_format_citations_multiple(self):
        citations = [
            Citation(
                document_id="doc-1",
                title="Budget 2025",
                source_url="https://example.com/a.csv",
                department="fin",
            ),
            Citation(
                document_id="doc-2",
                title="Labour Force",
                source_url="https://example.com/b.csv",
                department="statcan",
            ),
        ]
        result = format_citations(citations)
        assert "Budget 2025" in result
        assert "Labour Force" in result

    def test_format_citations_no_url(self):
        citations = [
            Citation(
                document_id="doc-1",
                title="No URL Doc",
                source_url="",
                department="fin",
            ),
        ]
        result = format_citations(citations)
        assert "No URL Doc" in result
        # URL line should not appear when source_url is empty
        assert "URL:" not in result

    def test_format_citations_no_table_title(self):
        citations = [
            Citation(
                document_id="doc-1",
                title="Simple Doc",
                source_url="https://example.com",
                department="fin",
                table_title=None,
            ),
        ]
        result = format_citations(citations)
        assert "Table:" not in result
