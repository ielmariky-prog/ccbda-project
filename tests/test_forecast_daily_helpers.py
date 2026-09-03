"""Tests for the daily forecast Lambda helpers (_next_business_days)."""

from __future__ import annotations

import pytest

from .conftest import get_forecast_daily_module

daily = get_forecast_daily_module()


class TestNextBusinessDays:
    def test_returns_n_business_days(self):
        # Thursday May 14 (a weekday) -> 7 next business days
        result = daily._next_business_days("2026-05-14T00:00:00Z", 7)
        assert len(result) == 7

    def test_skips_weekend_after_friday(self):
        # Friday May 15 -> next 5 business days are Mon-Fri
        result = daily._next_business_days("2026-05-15T00:00:00Z", 5)
        expected = [
            "2026-05-18T00:00:00Z",  # Mon
            "2026-05-19T00:00:00Z",  # Tue
            "2026-05-20T00:00:00Z",  # Wed
            "2026-05-21T00:00:00Z",  # Thu
            "2026-05-22T00:00:00Z",  # Fri
        ]
        assert result == expected

    def test_starts_from_weekend_input(self):
        # If we feed it a Saturday, the first business day returned should be Monday
        result = daily._next_business_days("2026-05-16T00:00:00Z", 1)
        assert result == ["2026-05-18T00:00:00Z"]

    def test_seven_days_crossing_two_weekends(self):
        # Friday May 15 -> 7 business days crosses one weekend (Sat-Sun May 16-17)
        result = daily._next_business_days("2026-05-15T00:00:00Z", 7)
        assert result == [
            "2026-05-18T00:00:00Z",  # Mon
            "2026-05-19T00:00:00Z",  # Tue
            "2026-05-20T00:00:00Z",  # Wed
            "2026-05-21T00:00:00Z",  # Thu
            "2026-05-22T00:00:00Z",  # Fri
            "2026-05-25T00:00:00Z",  # Mon
            "2026-05-26T00:00:00Z",  # Tue
        ]

    @pytest.mark.parametrize("ts", [
        "2026-05-18T00:00:00Z",  # Monday
        "2026-05-19T00:00:00Z",  # Tuesday
        "2026-05-22T00:00:00Z",  # Friday
    ])
    def test_returns_only_weekdays(self, ts):
        result = daily._next_business_days(ts, 10)
        from datetime import datetime
        for r in result:
            dt = datetime.fromisoformat(r.replace("Z", "+00:00"))
            assert dt.weekday() < 5, f"{r} is not a weekday"
