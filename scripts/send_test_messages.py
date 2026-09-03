"""
Send synthetic test messages to both SQS queues.

Prices queue (proj1102-prices-queue):
  - 100 slots per ticker, 5-minute spacing
  - ~15 % of slots randomly dropped  -> visible gaps for the Glue job to fill
  - ~10 % of slots sent twice with a slightly different price -> duplicates to deduplicate
  - every timestamp jittered by ±1-3 minutes so they never land exactly on a 5-min boundary

News queue (proj1102-news-queue):
  - ~10 headlines per ticker at random timestamps within the same window
  - fields: ticker, timestamp, headline, url, source

Usage:
  python scripts/send_test_messages.py
  python scripts/send_test_messages.py --dry-run          # print without sending
  python scripts/send_test_messages.py --slots 50         # fewer slots per ticker
"""

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone, timedelta

import boto3
from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config   = dotenv_values(os.path.join(ROOT_DIR, ".env"))

# Queue URLs - read from pipeline.json so there is a single source of truth
with open(os.path.join(ROOT_DIR, "config", "pipeline.json")) as fh:
    _pipeline = json.load(fh)

PRICES_QUEUE_URL = _pipeline["sources"]["prices"]["queue_url"]
NEWS_QUEUE_URL   = _pipeline["sources"]["news"]["queue_url"]
REGION           = config.get("AWS_DEFAULT_REGION", "eu-west-1")

TICKERS = ["AAPL", "TSLA", "BTC-USD", "ETH-USD"]

BASE_PRICES: dict = {
    "AAPL":    195.0,
    "TSLA":    220.0,
    "BTC-USD": 65_000.0,
    "ETH-USD": 3_200.0,
}

VOLATILITY: dict = {
    "AAPL":    0.0015,
    "TSLA":    0.003,
    "BTC-USD": 0.004,
    "ETH-USD": 0.005,
}

HEADLINES = [
    "{t} reports stronger-than-expected quarterly earnings",
    "Analysts raise {t} price target amid strong outlook",
    "Institutional investors increase {t} stake by 12 %",
    "{t} faces regulatory scrutiny over recent trading activity",
    "{t} announces major partnership deal",
    "Short sellers increase bets against {t}",
    "{t} hits 52-week high on renewed investor optimism",
    "CEO of {t} steps down; shares react sharply",
    "New product launch boosts {t} sentiment",
    "{t} downgraded by major investment bank",
    "Market volatility drags {t} lower despite solid fundamentals",
    "{t} acquires rival in all-stock deal",
]

SOURCES = ["newsapi", "reuters", "bloomberg", "apnews"]

# Tuning knobs
GAP_RATE       = 0.15   # fraction of slots silently dropped
DUPLICATE_RATE = 0.10   # fraction of slots sent a second time
JITTER_MIN     = 1      # minimum timestamp jitter in minutes
JITTER_MAX     = 3      # maximum timestamp jitter in minutes
NEWS_PER_TICKER = 10    # approximate news messages per ticker


# Generators

def random_walk(base: float, n: int, vol: float) -> list:
    price, out = base, []
    for _ in range(n):
        price *= 1 + random.gauss(0, vol)
        out.append(round(price, 4))
    return out


def jitter_ts(dt: datetime) -> datetime:
    """Add ±JITTER minutes so timestamps never land exactly on a 5-min boundary."""
    sign   = random.choice([-1, 1])
    offset = random.randint(JITTER_MIN, JITTER_MAX) * sign
    return dt + timedelta(minutes=offset)


def build_price_msg(ticker: str, price: float, ts: datetime, msg_idx: int) -> dict:
    return {
        "Id": f"price-{ticker}-{msg_idx}",
        "MessageBody": json.dumps({
            "ticker":    ticker,
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "price":     price,
            "volume":    max(0, int(random.gauss(1_000_000, 200_000))),
            "source":    "alpha_vantage",
        }),
    }


def build_news_msg(ticker: str, ts: datetime, msg_idx: int) -> dict:
    headline = random.choice(HEADLINES).format(t=ticker)
    return {
        "Id": f"news-{ticker}-{msg_idx}",
        "MessageBody": json.dumps({
            "ticker":    ticker,
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "headline":  headline,
            "url":       f"https://example.com/news/{ticker.lower()}-{msg_idx}",
            "source":    random.choice(SOURCES),
        }),
    }


def generate_price_messages(ticker: str, n_slots: int, start: datetime) -> tuple:
    """
    Returns (messages, n_gaps, n_duplicates).
    Each 'slot' is a 5-minute step; some are dropped (gaps), some sent twice (duplicates).
    Timestamps are jittered so they don't sit on clean boundaries.
    """
    prices     = random_walk(BASE_PRICES[ticker], n_slots, VOLATILITY[ticker])
    messages   = []
    n_gaps     = 0
    n_dupes    = 0
    msg_idx    = 0

    for i, price in enumerate(prices):
        slot_dt = start + timedelta(minutes=5 * i)

        if random.random() < GAP_RATE:
            n_gaps += 1
            continue

        ts = jitter_ts(slot_dt)
        messages.append(build_price_msg(ticker, price, ts, msg_idx))
        msg_idx += 1

        if random.random() < DUPLICATE_RATE:
            # Re-send the same logical slot - slightly different price, same jittered ts
            dup_price = round(price * (1 + random.uniform(-0.0005, 0.0005)), 4)
            messages.append(build_price_msg(ticker, dup_price, ts, msg_idx))
            msg_idx += 1
            n_dupes += 1

    return messages, n_gaps, n_dupes


def generate_news_messages(ticker: str, n: int, start: datetime, window_min: int) -> list:
    messages = []
    for i in range(n):
        offset = random.randint(0, window_min)
        ts     = start + timedelta(minutes=offset)
        messages.append(build_news_msg(ticker, ts, i))
    return messages


# SQS send

def send_batch(sqs_client, queue_url: str, messages: list, dry_run: bool) -> int:
    """Send messages in batches of 10. Returns number of messages successfully sent."""
    # Re-number IDs to ensure uniqueness within each batch of 10
    sent = 0
    for batch_start in range(0, len(messages), 10):
        batch = [
            {**m, "Id": str(j)}
            for j, m in enumerate(messages[batch_start : batch_start + 10])
        ]
        if dry_run:
            for m in batch:
                print(f"    [DRY-RUN] {m['MessageBody'][:120]} …")
            sent += len(batch)
            continue

        resp   = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)
        failed = resp.get("Failed", [])
        if failed:
            for f in failed:
                print(f"    WARNING: message {f['Id']} failed - {f.get('Message')}")
        sent += len(batch) - len(failed)
    return sent


# Main

def main() -> None:
    parser = argparse.ArgumentParser(description="Send synthetic test messages to both SQS queues")
    parser.add_argument("--slots",   type=int,  default=100,  help="5-min slots per ticker (default 100 ~ 8h)")
    parser.add_argument("--dry-run", action="store_true",     help="Print messages without sending")
    parser.add_argument("--start",   type=str,  default=None, help="ISO-8601 start time (default: now minus window)")
    args = parser.parse_args()

    window_min = args.slots * 5
    if args.start:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        start = datetime.now(timezone.utc) - timedelta(minutes=window_min)

    end = start + timedelta(minutes=window_min)
    print(f"Window : {start.strftime('%Y-%m-%dT%H:%M:%SZ')} -> {end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"Tickers: {TICKERS}")
    print(f"Slots  : {args.slots} per ticker  |  gap_rate={GAP_RATE}  dup_rate={DUPLICATE_RATE}  jitter=±{JITTER_MAX}min")
    print(f"Dry-run: {args.dry_run}\n")

    sqs = boto3.client("sqs", region_name=REGION)

    # Prices
    print("=== PRICES ===")
    total_price_sent = 0
    for ticker in TICKERS:
        msgs, n_gaps, n_dupes = generate_price_messages(ticker, args.slots, start)
        print(f"  {ticker:8s}  slots={args.slots}  gaps={n_gaps}  duplicates={n_dupes}  messages={len(msgs)}")
        sent = send_batch(sqs, PRICES_QUEUE_URL, msgs, args.dry_run)
        total_price_sent += sent

    # News
    print(f"\n=== NEWS ===")
    total_news_sent = 0
    for ticker in TICKERS:
        msgs = generate_news_messages(ticker, NEWS_PER_TICKER, start, window_min)
        print(f"  {ticker:8s}  messages={len(msgs)}")
        sent = send_batch(sqs, NEWS_QUEUE_URL, msgs, args.dry_run)
        total_news_sent += sent

    print(f"\nDone.  prices sent={total_price_sent}  news sent={total_news_sent}")


if __name__ == "__main__":
    main()
