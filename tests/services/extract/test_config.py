"""Tests for services/extract/config.py."""

from services.extract.config import ExtractConfig


class TestExtractConfig:
    def test_default_parser_version(self):
        cfg = ExtractConfig()
        assert cfg.parser_version == "0.1.0"

    def test_default_max_tables_per_document(self):
        cfg = ExtractConfig()
        assert cfg.max_tables_per_document == 200

    def test_default_min_rows_for_table(self):
        cfg = ExtractConfig()
        assert cfg.min_rows_for_table == 2

    def test_default_max_columns(self):
        cfg = ExtractConfig()
        assert cfg.max_columns == 500

    def test_custom_parser_version(self):
        cfg = ExtractConfig(parser_version="1.0.0")
        assert cfg.parser_version == "1.0.0"

    def test_custom_max_tables_per_document(self):
        cfg = ExtractConfig(max_tables_per_document=50)
        assert cfg.max_tables_per_document == 50

    def test_custom_min_rows_for_table(self):
        cfg = ExtractConfig(min_rows_for_table=5)
        assert cfg.min_rows_for_table == 5

    def test_custom_max_columns(self):
        cfg = ExtractConfig(max_columns=100)
        assert cfg.max_columns == 100

    def test_all_custom_values(self):
        cfg = ExtractConfig(
            parser_version="2.0.0",
            max_tables_per_document=10,
            min_rows_for_table=3,
            max_columns=50,
        )
        assert cfg.parser_version == "2.0.0"
        assert cfg.max_tables_per_document == 10
        assert cfg.min_rows_for_table == 3
        assert cfg.max_columns == 50
