"""Tests for the Glue job helpers: timestamp rounding and price-message parsing."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from .conftest import get_glue_module

glue = get_glue_module()


# to_utc - parse an ISO-8601 string (with Z) to a UTC datetime

class TestToUtc:
    def test_parses_z_suffix_to_utc(self):
        dt = glue.to_utc("2026-05-21T13:24:30Z")
        assert (dt.year, dt.month, dt.day) == (2026, 5, 21)
        assert (dt.hour, dt.minute, dt.second) == (13, 24, 30)
        assert dt.tzinfo is not None and dt.utcoffset().total_seconds() == 0


# round_to_interval - snap to the nearest 5-minute slot

class TestRoundToInterval:
    @pytest.mark.parametrize("hhmmss,expected", [
        ((13, 24, 30), (13, 25)),   # rounds up
        ((13, 22, 0),  (13, 20)),   # rounds down
        ((13, 22, 30), (13, 20)),   # half-slot: Python round() is banker's (160.5 -> 160)
        ((13, 0, 0),   (13, 0)),    # already on grid
    ])
    def test_rounds_to_nearest_5min(self, hhmmss, expected):
        h, m, s = hhmmss
        dt = datetime(2026, 5, 21, h, m, s, tzinfo=timezone.utc)
        out = glue.round_to_interval(dt, 5)
        assert (out.hour, out.minute) == expected
        assert out.second == 0 and out.microsecond == 0

    def test_end_of_day_does_not_overflow_to_hour_24(self):
        dt = datetime(2026, 5, 21, 23, 58, 0, tzinfo=timezone.utc)
        out = glue.round_to_interval(dt, 5)
        # 23:58 would round to 24:00; the helper clamps it back to 23:55
        assert (out.hour, out.minute) == (23, 55)


# fmt_ts - render back to the canonical ISO-8601 Z form, round-trip safe

class TestFmtTs:
    def test_roundtrip_is_stable(self):
        original = "2026-05-21T13:25:00Z"
        out = glue.fmt_ts(glue.round_to_interval(glue.to_utc(original), 5))
        assert out == original


# parse_price_messages - split SQS bodies into rows / ok / failed

class TestParsePriceMessages:
    def _msg(self, mid, body):
        return {"MessageId": mid, "Body": json.dumps(body), "ReceiptHandle": mid}

    def test_parses_valid_price_rows(self):
        msgs = [self._msg("a", {"ticker": "TSLA", "timestamp": "2026-05-21T13:25:00Z", "price": 417.1})]
        parsed, ok, failed = glue.parse_price_messages(msgs)
        assert failed == []
        assert len(parsed) == 1 and len(ok) == 1
        row = parsed[0]
        assert row["item_id"] == "TSLA"
        assert row["target_value"] == 417.1 and isinstance(row["target_value"], float)

    def test_accepts_a_payload_wrapper(self):
        body = {"payload": {"ticker": "MSFT", "timestamp": "2026-05-21T13:25:00Z", "price": 420}}
        parsed, ok, failed = glue.parse_price_messages([self._msg("b", body)])
        assert failed == [] and parsed[0]["item_id"] == "MSFT"

    def test_bad_message_goes_to_failed_not_parsed(self):
        good = self._msg("g", {"ticker": "NVDA", "timestamp": "2026-05-21T13:25:00Z", "price": 1.0})
        bad  = self._msg("b", {"ticker": "NVDA", "timestamp": "2026-05-21T13:25:00Z"})  # no price
        parsed, ok, failed = glue.parse_price_messages([good, bad])
        assert len(parsed) == 1 and len(ok) == 1
        assert len(failed) == 1 and failed[0]["MessageId"] == "b"
