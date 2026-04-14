"""Tests for shared/utils/hashing.py."""

import hashlib

import pytest

from shared.utils.hashing import compute_checksum, generate_document_id


class TestComputeChecksum:
    def test_deterministic_output(self):
        data = b"hello world"
        assert compute_checksum(data) == compute_checksum(data)

    def test_known_sha256(self):
        data = b"hello world"
        expected = hashlib.sha256(data).hexdigest()
        assert compute_checksum(data) == expected

    def test_empty_bytes(self):
        result = compute_checksum(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected

    def test_different_inputs_give_different_outputs(self):
        assert compute_checksum(b"abc") != compute_checksum(b"xyz")

    def test_returns_hex_string(self):
        result = compute_checksum(b"test")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest is 64 chars
        assert all(c in "0123456789abcdef" for c in result)


class TestGenerateDocumentId:
    def test_deterministic_output(self):
        url = "https://example.com/data.csv"
        checksum = "abc123"
        assert generate_document_id(url, checksum) == generate_document_id(url, checksum)

    def test_length_is_32(self):
        result = generate_document_id("https://example.com/data.csv", "checksum123")
        assert len(result) == 32

    def test_different_inputs_give_different_outputs(self):
        id1 = generate_document_id("https://example.com/a.csv", "checkA")
        id2 = generate_document_id("https://example.com/b.csv", "checkB")
        assert id1 != id2

    def test_different_url_same_checksum(self):
        id1 = generate_document_id("https://a.com/file.csv", "same_check")
        id2 = generate_document_id("https://b.com/file.csv", "same_check")
        assert id1 != id2

    def test_same_url_different_checksum(self):
        id1 = generate_document_id("https://example.com/file.csv", "check1")
        id2 = generate_document_id("https://example.com/file.csv", "check2")
        assert id1 != id2

    def test_known_value(self):
        url = "https://example.com/data.csv"
        checksum = "abc123"
        combined = f"{url}|{checksum}"
        expected = hashlib.sha256(combined.encode()).hexdigest()[:32]
        assert generate_document_id(url, checksum) == expected

    def test_returns_hex_string(self):
        result = generate_document_id("url", "checksum")
        assert all(c in "0123456789abcdef" for c in result)
