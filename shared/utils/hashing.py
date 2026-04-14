import hashlib


def compute_checksum(data: bytes) -> str:
    """SHA-256 hex digest of file contents."""
    return hashlib.sha256(data).hexdigest()


def generate_document_id(source_url: str, checksum: str) -> str:
    """Deterministic document ID from source URL and file checksum."""
    combined = f"{source_url}|{checksum}"
    return hashlib.sha256(combined.encode()).hexdigest()[:32]
