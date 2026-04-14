"""Tests for shared/utils/text_normalization.py."""

import pytest

from shared.utils.text_normalization import normalize_header, detect_language


class TestNormalizeHeader:
    def test_empty_string(self):
        assert normalize_header("") == ""

    def test_none_falsy(self):
        assert normalize_header("") == ""

    def test_strip_whitespace(self):
        assert normalize_header("  hello  ") == "hello"

    def test_collapse_whitespace(self):
        assert normalize_header("hello   world") == "hello world"

    def test_collapse_newlines(self):
        assert normalize_header("hello\nworld") == "hello world"

    def test_collapse_mixed_whitespace(self):
        assert normalize_header("hello\t\n  world") == "hello world"

    def test_lowercase(self):
        assert normalize_header("Hello World") == "hello world"

    def test_en_dash_to_hyphen(self):
        assert normalize_header("2023\u20132024") == "2023-2024"

    def test_em_dash_to_hyphen(self):
        assert normalize_header("2023\u20142024") == "2023-2024"

    def test_figure_dash_to_hyphen(self):
        assert normalize_header("2023\u20122024") == "2023-2024"

    def test_horizontal_bar_to_hyphen(self):
        assert normalize_header("2023\u20152024") == "2023-2024"

    def test_smart_single_quotes(self):
        assert normalize_header("it\u2018s") == "it's"
        assert normalize_header("it\u2019s") == "it's"

    def test_smart_double_quotes(self):
        assert normalize_header("\u201chello\u201d") == '"hello"'

    def test_bom_removal(self):
        assert normalize_header("\ufeffhello") == "hello"

    def test_bom_in_middle(self):
        assert normalize_header("he\ufeffllo") == "hello"

    def test_percent_standardization(self):
        assert normalize_header("( % )") == "(%)"
        assert normalize_header("(  %  )") == "(%)"

    def test_percent_already_clean(self):
        assert normalize_header("(%)") == "(%)"

    def test_dollar_m_standardization(self):
        assert normalize_header("m$") == "millions $"

    def test_dollar_us_standardization(self):
        assert normalize_header("$us") == "$ us"

    def test_trailing_colon_removal(self):
        assert normalize_header("Total:") == "total"

    def test_multiple_trailing_colons(self):
        assert normalize_header("Total:::") == "total"

    def test_goc_header_change_percent(self):
        assert normalize_header("Change(%)") == "change(%)"

    def test_goc_header_transferts_federaux(self):
        assert normalize_header("Transferts\nf\u00e9d\u00e9raux") == "transferts f\u00e9d\u00e9raux"

    def test_goc_header_fiscal_year_en_dash(self):
        assert normalize_header("2023\u20132024") == "2023-2024"

    def test_combined_transformations(self):
        raw = "  \ufeff Change ( % ) : "
        # BOM removed, whitespace collapsed, lowercased, percent standardized, trailing colon stripped
        result = normalize_header(raw)
        assert result == "change (%)"


class TestDetectLanguage:
    def test_empty_string(self):
        assert detect_language("") == "unknown"

    def test_english_text(self):
        result = detect_language("The total revenue for the fiscal year")
        assert result == "en"

    def test_french_text_with_accents(self):
        result = detect_language("R\u00e9sum\u00e9 des d\u00e9penses")
        assert result == "fr"

    def test_french_text_with_indicators(self):
        result = detect_language("Les transferts dans les provinces")
        assert result == "fr"

    def test_bilingual_text(self):
        result = detect_language("The total des transferts for the provinces dans les regions")
        assert result == "bilingual"

    def test_plain_english_no_indicators(self):
        result = detect_language("Revenue growth rate")
        assert result == "en"

    def test_french_chars_only(self):
        # Has French character but no English indicators => fr
        result = detect_language("\u00e9conomie")
        assert result == "fr"

    def test_single_french_indicator_not_enough(self):
        # Only 1 French indicator word, no French chars => en
        result = detect_language("les")
        assert result == "en"

    def test_two_french_indicators_is_french(self):
        result = detect_language("les dans")
        assert result == "fr"

    def test_bilingual_requires_two_english_indicators(self):
        # French chars + only 1 English indicator => fr
        result = detect_language("\u00e9conomie the")
        assert result == "fr"

    def test_bilingual_with_two_english_indicators(self):
        # French chars + 2 English indicators => bilingual
        result = detect_language("\u00e9conomie the and")
        assert result == "bilingual"
