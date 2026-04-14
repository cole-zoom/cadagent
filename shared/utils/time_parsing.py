"""Parsers for time-like values found in GoC data headers.

Handles fiscal years (2023-24, 2023-2024), calendar years (2023),
months (January 2024), time ranges (April to December 2023-24),
relative years (Year 1, Year 2), and composite headers
like 'Projection 2024-2025'.
"""

import re
from dataclasses import dataclass

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9,
    "oct": 10, "nov": 11, "dec": 12,
    # French
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}

SCENARIO_KEYWORDS = {"projection", "forecast", "actual", "historical", "baseline", "prévision"}

# Fiscal year starts April 1 for GoC
FISCAL_YEAR_START_MONTH = 4


@dataclass
class ParsedTime:
    time_type: str  # year, fiscal_year, quarter, month, date, range
    label: str
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    fiscal_year_start_month: int = FISCAL_YEAR_START_MONTH
    is_projection: bool = False
    scenario_hint: str | None = None


def parse_fiscal_year(s: str) -> ParsedTime | None:
    """Parse fiscal year patterns: 2023-24, 2023-2024, 2023--2024."""
    text = s.strip().lower()

    # Strip scenario prefix if present
    scenario = None
    for kw in SCENARIO_KEYWORDS:
        if text.startswith(kw):
            scenario = kw
            text = text[len(kw):].strip()
            break

    # Pattern: YYYY-YY or YYYY-YYYY
    match = re.match(r"^(\d{4})[-\u2013\u2014](\d{2,4})$", text)
    if match:
        start_year = int(match.group(1))
        end_part = match.group(2)
        if len(end_part) == 2:
            end_year = int(str(start_year)[:2] + end_part)
        else:
            end_year = int(end_part)

        if end_year == start_year + 1:
            return ParsedTime(
                time_type="fiscal_year",
                label=f"{start_year}-{str(end_year)[-2:]}",
                start_date=f"{start_year}-04-01",
                end_date=f"{end_year}-03-31",
                is_projection=scenario in ("projection", "forecast", "prévision"),
                scenario_hint=scenario,
            )

    return None


def parse_calendar_year(s: str) -> ParsedTime | None:
    """Parse a standalone calendar year: 2023, 2024."""
    text = s.strip()

    scenario = None
    lower = text.lower()
    for kw in SCENARIO_KEYWORDS:
        if lower.startswith(kw):
            scenario = kw
            text = text[len(kw):].strip()
            break

    match = re.match(r"^(\d{4})$", text)
    if match:
        year = int(match.group(1))
        if 1900 <= year <= 2100:
            return ParsedTime(
                time_type="year",
                label=str(year),
                start_date=f"{year}-01-01",
                end_date=f"{year}-12-31",
                is_projection=scenario in ("projection", "forecast", "prévision"),
                scenario_hint=scenario,
            )
    return None


def parse_month(s: str) -> ParsedTime | None:
    """Parse month patterns: January 2024, Jan 2024, janvier 2024."""
    text = s.strip().lower()
    match = re.match(r"^([a-zéûî]+)\.?\s+(\d{4})$", text)
    if match:
        month_name = match.group(1)
        year = int(match.group(2))
        month_num = MONTH_MAP.get(month_name)
        if month_num and 1900 <= year <= 2100:
            import calendar
            last_day = calendar.monthrange(year, month_num)[1]
            return ParsedTime(
                time_type="month",
                label=f"{year}-{month_num:02d}",
                start_date=f"{year}-{month_num:02d}-01",
                end_date=f"{year}-{month_num:02d}-{last_day:02d}",
            )
    return None


def parse_time_range(s: str) -> ParsedTime | None:
    """Parse time range patterns: 'April to December 2023-24'."""
    text = s.strip().lower()
    match = re.match(
        r"^([a-zéûî]+)\s+to\s+([a-zéûî]+)\s+(\d{4}[-\u2013]\d{2,4})$", text
    )
    if match:
        start_month_name = match.group(1)
        end_month_name = match.group(2)
        year_part = match.group(3)

        start_month = MONTH_MAP.get(start_month_name)
        end_month = MONTH_MAP.get(end_month_name)

        if start_month and end_month:
            fy = parse_fiscal_year(year_part)
            if fy:
                start_year = int(fy.start_date[:4])
                end_year = start_year if end_month >= start_month else start_year + 1
                import calendar
                last_day = calendar.monthrange(end_year, end_month)[1]
                return ParsedTime(
                    time_type="range",
                    label=f"{start_month_name} to {end_month_name} {year_part}",
                    start_date=f"{start_year}-{start_month:02d}-01",
                    end_date=f"{end_year}-{end_month:02d}-{last_day:02d}",
                )
    return None


def parse_relative_year(s: str) -> ParsedTime | None:
    """Parse relative year patterns: Year 1, Year 2, Year 5."""
    text = s.strip().lower()
    match = re.match(r"^year\s+(\d+)$", text)
    if match:
        year_num = int(match.group(1))
        return ParsedTime(
            time_type="range",
            label=f"Year {year_num}",
            start_date="",  # Must be resolved using document context
            end_date="",
        )
    return None


def parse_time(s: str) -> ParsedTime | None:
    """Try all time parsers in order. Returns the first match or None."""
    for parser in [
        parse_fiscal_year,
        parse_calendar_year,
        parse_month,
        parse_time_range,
        parse_relative_year,
    ]:
        result = parser(s)
        if result is not None:
            return result
    return None


def is_time_like(s: str) -> bool:
    """Quick check: does this string look like a time value?"""
    return parse_time(s) is not None
