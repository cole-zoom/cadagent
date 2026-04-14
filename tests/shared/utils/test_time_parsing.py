"""Tests for shared/utils/time_parsing.py."""

import pytest

from shared.utils.time_parsing import (
    ParsedTime,
    parse_fiscal_year,
    parse_calendar_year,
    parse_month,
    parse_time_range,
    parse_relative_year,
    parse_time,
    is_time_like,
)


class TestParseFiscalYear:
    def test_short_form(self):
        result = parse_fiscal_year("2023-24")
        assert result is not None
        assert result.time_type == "fiscal_year"
        assert result.label == "2023-24"
        assert result.start_date == "2023-04-01"
        assert result.end_date == "2024-03-31"

    def test_long_form(self):
        result = parse_fiscal_year("2023-2024")
        assert result is not None
        assert result.time_type == "fiscal_year"
        assert result.label == "2023-24"
        assert result.start_date == "2023-04-01"
        assert result.end_date == "2024-03-31"

    def test_en_dash(self):
        result = parse_fiscal_year("2023\u20132024")
        assert result is not None
        assert result.time_type == "fiscal_year"
        assert result.start_date == "2023-04-01"
        assert result.end_date == "2024-03-31"

    def test_em_dash(self):
        result = parse_fiscal_year("2023\u20142024")
        assert result is not None
        assert result.time_type == "fiscal_year"

    def test_scenario_prefix_projection(self):
        result = parse_fiscal_year("Projection 2024-2025")
        assert result is not None
        assert result.time_type == "fiscal_year"
        assert result.start_date == "2024-04-01"
        assert result.end_date == "2025-03-31"
        assert result.is_projection is True
        assert result.scenario_hint == "projection"

    def test_scenario_prefix_forecast(self):
        result = parse_fiscal_year("forecast 2024-25")
        assert result is not None
        assert result.is_projection is True
        assert result.scenario_hint == "forecast"

    def test_scenario_prefix_actual(self):
        result = parse_fiscal_year("actual 2023-24")
        assert result is not None
        assert result.is_projection is False
        assert result.scenario_hint == "actual"

    def test_scenario_prefix_prevision(self):
        result = parse_fiscal_year("pr\u00e9vision 2024-25")
        assert result is not None
        assert result.is_projection is True
        assert result.scenario_hint == "pr\u00e9vision"

    def test_non_consecutive_years_returns_none(self):
        assert parse_fiscal_year("2023-2025") is None

    def test_same_year_returns_none(self):
        assert parse_fiscal_year("2023-2023") is None

    def test_non_fiscal_year_string(self):
        assert parse_fiscal_year("Canada") is None

    def test_whitespace_handling(self):
        result = parse_fiscal_year("  2023-24  ")
        assert result is not None


class TestParseCalendarYear:
    def test_valid_year(self):
        result = parse_calendar_year("2023")
        assert result is not None
        assert result.time_type == "year"
        assert result.label == "2023"
        assert result.start_date == "2023-01-01"
        assert result.end_date == "2023-12-31"

    def test_out_of_range_low(self):
        assert parse_calendar_year("1800") is None

    def test_out_of_range_high(self):
        assert parse_calendar_year("2200") is None

    def test_boundary_low(self):
        result = parse_calendar_year("1900")
        assert result is not None

    def test_boundary_high(self):
        result = parse_calendar_year("2100")
        assert result is not None

    def test_scenario_prefix(self):
        result = parse_calendar_year("projection 2024")
        assert result is not None
        assert result.is_projection is True
        assert result.scenario_hint == "projection"

    def test_non_year_string(self):
        assert parse_calendar_year("Women") is None

    def test_not_standalone_year(self):
        # This shouldn't match because it's not just a year
        assert parse_calendar_year("2023-24") is None


class TestParseMonth:
    def test_english_full_month(self):
        result = parse_month("January 2024")
        assert result is not None
        assert result.time_type == "month"
        assert result.label == "2024-01"
        assert result.start_date == "2024-01-01"
        assert result.end_date == "2024-01-31"

    def test_english_abbreviated_month(self):
        result = parse_month("Jan 2024")
        assert result is not None
        assert result.time_type == "month"
        assert result.start_date == "2024-01-01"

    def test_french_month(self):
        result = parse_month("janvier 2024")
        assert result is not None
        assert result.time_type == "month"
        assert result.start_date == "2024-01-01"

    def test_french_month_with_accent(self):
        result = parse_month("f\u00e9vrier 2024")
        assert result is not None
        assert result.time_type == "month"
        assert result.start_date == "2024-02-01"
        assert result.end_date == "2024-02-29"  # 2024 is a leap year

    def test_february_non_leap_year(self):
        result = parse_month("February 2023")
        assert result is not None
        assert result.end_date == "2023-02-28"

    def test_december(self):
        result = parse_month("December 2024")
        assert result is not None
        assert result.end_date == "2024-12-31"

    def test_invalid_month_name(self):
        assert parse_month("Foobar 2024") is None

    def test_case_insensitive(self):
        result = parse_month("JANUARY 2024")
        assert result is not None


class TestParseTimeRange:
    def test_april_to_december(self):
        result = parse_time_range("April to December 2023-24")
        assert result is not None
        assert result.time_type == "range"
        assert result.start_date == "2023-04-01"
        assert result.end_date == "2023-12-31"

    def test_april_to_march_wraps_year(self):
        result = parse_time_range("April to March 2023-24")
        assert result is not None
        assert result.start_date == "2023-04-01"
        assert result.end_date == "2024-03-31"

    def test_label_preserved(self):
        result = parse_time_range("April to December 2023-24")
        assert result is not None
        assert "april" in result.label
        assert "december" in result.label

    def test_invalid_months(self):
        assert parse_time_range("Foo to Bar 2023-24") is None

    def test_no_fiscal_year(self):
        assert parse_time_range("April to December 2023") is None


class TestParseRelativeYear:
    def test_year_1(self):
        result = parse_relative_year("Year 1")
        assert result is not None
        assert result.time_type == "range"
        assert result.label == "Year 1"
        assert result.start_date == ""
        assert result.end_date == ""

    def test_year_5(self):
        result = parse_relative_year("Year 5")
        assert result is not None
        assert result.label == "Year 5"

    def test_case_insensitive(self):
        result = parse_relative_year("year 3")
        assert result is not None
        assert result.label == "Year 3"

    def test_non_relative_year(self):
        assert parse_relative_year("2023") is None

    def test_invalid_string(self):
        assert parse_relative_year("Canada") is None


class TestParseTime:
    def test_fiscal_year(self):
        result = parse_time("2023-24")
        assert result is not None
        assert result.time_type == "fiscal_year"

    def test_calendar_year(self):
        result = parse_time("2023")
        assert result is not None
        assert result.time_type == "year"

    def test_month(self):
        result = parse_time("January 2024")
        assert result is not None
        assert result.time_type == "month"

    def test_range(self):
        result = parse_time("April to December 2023-24")
        assert result is not None
        assert result.time_type == "range"

    def test_relative_year(self):
        result = parse_time("Year 1")
        assert result is not None
        assert result.time_type == "range"

    def test_non_time_returns_none(self):
        assert parse_time("Canada") is None

    def test_priority_fiscal_over_calendar(self):
        # "2023-24" should parse as fiscal, not calendar
        result = parse_time("2023-24")
        assert result.time_type == "fiscal_year"


class TestIsTimeLike:
    def test_fiscal_year(self):
        assert is_time_like("2023-24") is True

    def test_calendar_year(self):
        assert is_time_like("2023") is True

    def test_month(self):
        assert is_time_like("January 2024") is True

    def test_relative_year(self):
        assert is_time_like("Year 1") is True

    def test_non_time_canada(self):
        assert is_time_like("Canada") is False

    def test_non_time_women(self):
        assert is_time_like("Women") is False

    def test_non_time_empty(self):
        assert is_time_like("") is False

    def test_non_time_random_text(self):
        assert is_time_like("Total revenue") is False
