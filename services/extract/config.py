from dataclasses import dataclass


@dataclass
class ExtractConfig:
    parser_version: str = "0.1.0"
    max_tables_per_document: int = 200
    min_rows_for_table: int = 2
    max_columns: int = 500
