"""Tests for services/ingest/config.py."""

from services.ingest.config import IngestConfig


class TestIngestConfig:
    def test_default_batch_size(self):
        cfg = IngestConfig()
        assert cfg.batch_size == 50

    def test_default_max_file_size_mb(self):
        cfg = IngestConfig()
        assert cfg.max_file_size_mb == 500

    def test_default_retry_count(self):
        cfg = IngestConfig()
        assert cfg.retry_count == 3

    def test_default_rate_limit_delay(self):
        cfg = IngestConfig()
        assert cfg.rate_limit_delay == 0.5

    def test_custom_batch_size(self):
        cfg = IngestConfig(batch_size=100)
        assert cfg.batch_size == 100

    def test_custom_max_file_size_mb(self):
        cfg = IngestConfig(max_file_size_mb=1024)
        assert cfg.max_file_size_mb == 1024

    def test_custom_retry_count(self):
        cfg = IngestConfig(retry_count=5)
        assert cfg.retry_count == 5

    def test_custom_rate_limit_delay(self):
        cfg = IngestConfig(rate_limit_delay=1.0)
        assert cfg.rate_limit_delay == 1.0

    def test_all_custom_values(self):
        cfg = IngestConfig(batch_size=200, max_file_size_mb=10, retry_count=1, rate_limit_delay=0.1)
        assert cfg.batch_size == 200
        assert cfg.max_file_size_mb == 10
        assert cfg.retry_count == 1
        assert cfg.rate_limit_delay == 0.1
