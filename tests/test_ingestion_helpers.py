"""Tests for the ingestion Lambda helpers: key rotation and message shapes."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from .conftest import get_ingestion_module

ing = get_ingestion_module()


# get_api_key - deterministic rotation by the UTC hour

class TestGetApiKey:
    @pytest.mark.parametrize("symbol", ["TSLA", "MSFT", "NVDA"])
    def test_returns_a_key_from_that_symbols_pool(self, symbol):
        key = ing.get_api_key(symbol)
        assert key in ing.API_KEYS[symbol]

    @pytest.mark.parametrize("hour,expected_index", [
        (0, 0),    # (0*2) % 3 = 0
        (1, 2),    # (1*2) % 3 = 2
        (3, 0),    # (3*2) % 3 = 0
        (10, 2),   # (10*2) % 3 = 2
        (12, 0),   # (12*2) % 3 = 0
    ])
    def test_rotation_index_follows_the_hour(self, monkeypatch, hour, expected_index):
        class _FixedDatetime:
            @staticmethod
            def now(tz=None):
                return datetime(2026, 5, 21, hour, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(ing, "datetime", _FixedDatetime)
        assert ing.get_api_key("TSLA") == ing.API_KEYS["TSLA"][expected_index]


# fetch_price - message shape and validation

class TestFetchPrice:
    def test_builds_a_well_formed_price_message(self, monkeypatch):
        monkeypatch.setattr(ing, "call_api", lambda url: {
            "Global Quote": {"05. price": "417.11", "06. volume": "1234567"}
        })
        msg = ing.fetch_price("TSLA")
        assert msg["ticker"] == "TSLA"
        assert msg["price"] == 417.11 and isinstance(msg["price"], float)
        assert msg["volume"] == 1234567 and isinstance(msg["volume"], int)
        assert msg["source"] == "alpha_vantage"
        # timestamp is ISO-8601 UTC with a trailing Z
        assert msg["timestamp"].endswith("Z")
        datetime.strptime(msg["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

    def test_raises_when_quote_is_empty(self, monkeypatch):
        monkeypatch.setattr(ing, "call_api", lambda url: {"Global Quote": {}})
        with pytest.raises(ValueError):
            ing.fetch_price("MSFT")


# fetch_news - shape and the 5-article cap

class TestFetchNews:
    def test_maps_articles_and_caps_at_five(self, monkeypatch):
        feed = [{"title": f"h{i}", "url": f"http://x/{i}"} for i in range(8)]
        monkeypatch.setattr(ing, "call_api", lambda url: {"feed": feed})
        out = ing.fetch_news("NVDA")
        assert len(out) == 5                      # capped at 5
        assert {r["ticker"] for r in out} == {"NVDA"}
        assert out[0]["headline"] == "h0"
        assert out[0]["source"] == "alpha_vantage"

    def test_empty_feed_returns_empty_list(self, monkeypatch):
        monkeypatch.setattr(ing, "call_api", lambda url: {})
        assert ing.fetch_news("TSLA") == []
