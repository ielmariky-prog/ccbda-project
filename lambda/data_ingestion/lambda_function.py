import json
import os
import time
import boto3
import urllib.request
from datetime import datetime, timezone

ASSETS = json.loads(os.environ.get('ASSETS', '["TSLA","MSFT","NVDA"]'))
PRICES_QUEUE_URL = os.environ['PRICESQUEUE']
NEWS_QUEUE_URL   = os.environ['NEWSQUEUE']

API_KEYS = {
    "TSLA": [os.environ['APIKEY1'], os.environ['APIKEY4'], os.environ['APIKEY7']],
    "MSFT": [os.environ['APIKEY2'], os.environ['APIKEY5'], os.environ['APIKEY8']],
    "NVDA": [os.environ['APIKEY3'], os.environ['APIKEY6'], os.environ['APIKEY9']],
}

def get_api_key(symbol):
    keys = API_KEYS[symbol]
    index = (datetime.now(timezone.utc).hour * 2) % len(keys)
    return keys[index]

sqs = boto3.client('sqs', region_name=os.environ.get('APPREGION', 'eu-west-1'))


def call_api(url):
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode())


def fetch_price(symbol):
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=GLOBAL_QUOTE"
        f"&symbol={symbol}"
        f"&apikey={get_api_key(symbol)}"
    )
    data  = call_api(url)
    quote = data.get("Global Quote", {})
    if not quote:
        raise ValueError(f"No price data returned for {symbol}")
    return {
        "ticker":    symbol,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price":     float(quote["05. price"]),
        "volume":    int(quote["06. volume"]),
        "source":    "alpha_vantage",
    }


def fetch_news(symbol):
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=NEWS_SENTIMENT"
        f"&tickers={symbol}"
        f"&limit=5"
        f"&apikey={get_api_key(symbol)}"
    )
    data     = call_api(url)
    articles = data.get("feed", [])

    results = []
    for article in articles[:5]:
        results.append({
            "ticker":    symbol,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "headline":  article.get("title"),
            "url":       article.get("url"),
            "source":    "alpha_vantage",
        })
    return results


def send_to_sqs(queue_url, message):
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
    )


def lambda_handler(event, context):
    results = []
    for symbol in ASSETS:
        try:
            # Prix -> SQS prices queue
            price_msg = fetch_price(symbol)
            send_to_sqs(PRICES_QUEUE_URL, price_msg)
            print(f"[OK] {symbol} price @ {price_msg['price']} sent to prices queue")

            # Respecter la limite Alpha Vantage de 1 req/seconde
            time.sleep(1.2)

            # News -> SQS news queue (un message par article)
            news_list = fetch_news(symbol)
            for news_msg in news_list:
                send_to_sqs(NEWS_QUEUE_URL, news_msg)
            print(f"[OK] {symbol} news ({len(news_list)} articles) sent to news queue")

            results.append({
                "symbol":   symbol,
                "status":   "ok",
                "price":    price_msg["price"],
                "articles": len(news_list),
            })

        except Exception as e:
            results.append({"symbol": symbol, "status": "error", "message": str(e)})
            print(f"[ERROR] {symbol}: {e}")

    return {"statusCode": 200, "body": json.dumps(results)}
