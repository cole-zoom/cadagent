import logging

from google.cloud import storage

logger = logging.getLogger(__name__)


class GcsClient:
    def __init__(self, project_id: str):
        self.client = storage.Client(project=project_id)

    def upload_raw_file(
        self,
        bucket_name: str,
        department: str,
        year: str,
        document_id: str,
        filename: str,
        data: bytes,
    ) -> str:
        """Upload a raw file to GCS and return the GCS URI."""
        path = f"raw/goc/department={department}/year={year}/document_id={document_id}/{filename}"
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(path)
        blob.upload_from_string(data)
        gcs_uri = f"gs://{bucket_name}/{path}"
        logger.info("Uploaded %s (%d bytes)", gcs_uri, len(data))
        return gcs_uri

    def upload_processed_file(
        self,
        bucket_name: str,
        subpath: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload an extracted/processed artifact to GCS."""
        path = f"processed/{subpath}"
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(path)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{bucket_name}/{path}"

    def file_exists(self, gcs_uri: str) -> bool:
        """Check if a GCS object exists."""
        bucket_name, blob_path = self._parse_uri(gcs_uri)
        bucket = self.client.bucket(bucket_name)
        return bucket.blob(blob_path).exists()

    def download_file(self, gcs_uri: str) -> bytes:
        """Download a file from GCS."""
        bucket_name, blob_path = self._parse_uri(gcs_uri)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        return blob.download_as_bytes()

    @staticmethod
    def _parse_uri(gcs_uri: str) -> tuple[str, str]:
        """Parse gs://bucket/path into (bucket, path)."""
        if not gcs_uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {gcs_uri}")
        parts = gcs_uri[5:].split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""
