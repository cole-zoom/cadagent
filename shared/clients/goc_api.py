import logging
import time
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

GOC_BASE = "https://open.canada.ca"


class GocApiClient:
    def __init__(self, base_url: str, rate_limit_delay: float = 0.5):
        self.base_url = base_url.rstrip("/")
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()

    def _get(self, action: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}/{action}"
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"CKAN API error: {body.get('error', body)}")
        return body["result"]

    def search_datasets(
        self,
        organization: str,
        rows: int = 100,
        start: int = 0,
        fq: str | None = None,
    ) -> tuple[list[dict], int]:
        """Search datasets for an organization. Returns (datasets, total_count)."""
        filter_query = f"organization:{organization}"
        if fq:
            filter_query = f"{filter_query}+{fq}"

        result = self._get(
            "package_search",
            params={"fq": filter_query, "rows": rows, "start": start},
        )
        return result["results"], result["count"]

    def search_all_datasets(self, organization: str) -> list[dict]:
        """Paginate through all datasets for an organization."""
        all_datasets: list[dict] = []
        start = 0
        page_size = 100

        while True:
            datasets, total = self.search_datasets(organization, rows=page_size, start=start)
            all_datasets.extend(datasets)
            logger.info(
                "Fetched %d/%d datasets for %s", len(all_datasets), total, organization
            )

            if start + page_size >= total:
                break
            start += page_size
            time.sleep(self.rate_limit_delay)

        return all_datasets

    def get_dataset(self, dataset_id: str) -> dict:
        """Fetch full metadata and resources for a single dataset."""
        return self._get("package_show", params={"id": dataset_id})

    def list_resources(self, dataset: dict) -> list[dict]:
        """Extract resources from a dataset object."""
        return dataset.get("resources", [])

    def download_resource(self, url: str) -> bytes:
        """Download a resource file, handling relative URLs."""
        if url.startswith("/"):
            url = urljoin(GOC_BASE, url)

        time.sleep(self.rate_limit_delay)
        resp = self.session.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        # Check Content-Length before downloading the full body
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > 100 * 1024 * 1024:
            resp.close()
            raise ValueError(f"File too large ({int(content_length)} bytes), skipping")

        return resp.content

    @staticmethod
    def extract_title(dataset: dict) -> str:
        """Get the English title from a dataset, handling bilingual fields."""
        translated = dataset.get("title_translated")
        if isinstance(translated, dict) and translated.get("en"):
            return translated["en"]
        return dataset.get("title", "")

    @staticmethod
    def extract_language(resource: dict) -> str:
        """Detect language from a resource object."""
        lang = resource.get("language")
        if isinstance(lang, list):
            if "en" in lang and "fr" in lang:
                return "bilingual"
            return lang[0] if lang else "unknown"
        if isinstance(lang, str):
            return lang
        return "unknown"

    @staticmethod
    def extract_format(resource: dict) -> str:
        """Normalize file format from a resource."""
        fmt = resource.get("format", "").strip().upper()
        format_map = {
            "XLS": "xls",
            "XLSX": "xlsx",
            "CSV": "csv",
            "PDF": "pdf",
            "XML": "xml",
            "HTML": "html",
            "JSON": "json",
            "ZIP": "zip",
        }
        return format_map.get(fmt, fmt.lower())
