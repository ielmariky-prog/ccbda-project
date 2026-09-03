"""Tests for the hourly forecast Lambda helpers."""

from __future__ import annotations

from decimal import Decimal

import pytest

from .conftest import get_forecast_module

forecast = get_forecast_module()


class TestIsMarketOpen:
    @pytest.mark.parametrize("ts", [
        "2026-05-19T13:30:00Z",   # open (Tuesday)
        "2026-05-19T16:00:00Z",   # mid-session
        "2026-05-19T19:59:00Z",   # just before close
    ])
    def test_open_during_regular_hours_weekday(self, ts):
        assert forecast._is_market_open(ts) is True

    @pytest.mark.parametrize("ts", [
        "2026-05-19T13:29:00Z",   # before open
        "2026-05-19T20:00:00Z",   # close (< 20:00 rule)
        "2026-05-19T22:00:00Z",   # after hours
        "2026-05-19T05:00:00Z",   # pre-open
    ])
    def test_closed_outside_regular_hours_weekday(self, ts):
        assert forecast._is_market_open(ts) is False

    @pytest.mark.parametrize("ts", [
        "2026-05-16T15:00:00Z",   # Saturday
        "2026-05-17T15:00:00Z",   # Sunday
    ])
    def test_closed_on_weekends(self, ts):
        assert forecast._is_market_open(ts) is False


class TestSeverity:
    def test_inside_band_is_low(self):
        assert forecast._severity(actual=100.0, p10=95.0, p90=105.0) == "low"

    def test_at_band_edges_is_low(self):
        assert forecast._severity(actual=95.0,  p10=95.0, p90=105.0) == "low"
        assert forecast._severity(actual=105.0, p10=95.0, p90=105.0) == "low"

    def test_just_outside_band_is_medium(self):
        assert forecast._severity(actual=94.0,  p10=95.0, p90=105.0) == "medium"
        assert forecast._severity(actual=106.0, p10=95.0, p90=105.0) == "medium"

    def test_far_outside_band_is_high(self):
        assert forecast._severity(actual=85.0,  p10=95.0, p90=105.0) == "high"
        assert forecast._severity(actual=115.0, p10=95.0, p90=105.0) == "high"

    def test_zero_band_width_does_not_divide_by_zero(self):
        assert forecast._severity(actual=100.0, p10=100.0, p90=100.0) == "low"
        assert forecast._severity(actual=100.1, p10=100.0, p90=100.0) in ("medium", "high")


class TestToDecimal:
    def test_int_converts(self):
        assert forecast._to_decimal(5) == Decimal("5.0000")

    def test_float_rounds_to_4dp(self):
        assert forecast._to_decimal(1.234567) == Decimal("1.2346")

    def test_negative(self):
        assert forecast._to_decimal(-2.5) == Decimal("-2.5000")


class TestAddMinutes:
    def test_add_5_min(self):
        assert forecast._add_minutes("2026-05-19T10:00:00Z", 5) == "2026-05-19T10:05:00Z"

    def test_crosses_hour(self):
        assert forecast._add_minutes("2026-05-19T10:58:00Z", 5) == "2026-05-19T11:03:00Z"

    def test_crosses_midnight(self):
        assert forecast._add_minutes("2026-05-19T23:55:00Z", 10) == "2026-05-20T00:05:00Z"

    def test_handles_space_separator(self):
        assert forecast._add_minutes("2026-05-19 10:00:00Z", 30) == "2026-05-19T10:30:00Z"
