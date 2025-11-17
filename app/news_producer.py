#!/usr/bin/env python3
"""
Simple NewsAPI -> Kafka producer.

Usage:
    python -m app.news_producer --bootstrap-servers localhost:9092 --topic news-articles --q bitcoin

Reads NEWSAPI_KEY from environment or .env file. Avoid committing API keys to source control.
"""

import os
import time
import signal
import argparse
import logging
from collections import OrderedDict
import json

from newsapi import NewsApiClient
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

logger = logging.getLogger("news_producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Simple LRU set based on OrderedDict
class LRUSet:
    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize
        self._data = OrderedDict()

    def add(self, key: str) -> bool:
        """Add key if not present. Return True if added (was new), False if it existed."""
        existed = key in self._data
        if existed:
            # move to end (most recent)
            self._data.move_to_end(key)
            return False
        self._data[key] = None
        if len(self._data) > self.maxsize:
            self._data.popitem(last=False)
        return True

    def __contains__(self, key: str) -> bool:
        return key in self._data


def create_producer(bootstrap_servers: str) -> Producer:
    conf = {"bootstrap.servers": bootstrap_servers}
    p = Producer(conf)
    return p


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for message: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def fetch_articles(newsapi: NewsApiClient, q: str, page: int = 1, page_size: int = 20):
    # Use everything endpoint to search across sources and domains
    try:
        res = newsapi.get_everything(q=q, page=page, page_size=page_size, language="en", sort_by="publishedAt")
        articles = res.get("articles") or []
        return articles
    except Exception:
        logger.exception("Error fetching articles from NewsAPI")
        return []


def main():
    parser = argparse.ArgumentParser(description="NewsAPI -> Kafka producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_1", "topic1"))
    # --q is optional now; if not provided the default 'bitcoin' (or NEWSAPI_DEFAULT_QUERY) will be used
    parser.add_argument("--q", default=os.getenv("NEWSAPI_DEFAULT_QUERY", "bitcoin"), help="Search query for NewsAPI, e.g. bitcoin")
    parser.add_argument("--poll-interval", type=int, default=int(os.getenv("POLL_INTERVAL", "30")), help="Seconds between polls")
    parser.add_argument("--page-size", type=int, default=20, help="How many articles per page (max 100)")
    parser.add_argument("--dedup-maxsize", type=int, default=int(os.getenv("DEDUP_MAXSIZE", "1000")), help="LRU dedupe size")
    args = parser.parse_args()

    newsapi_key = os.getenv("NEWSAPI_KEY")
    if not newsapi_key:
        logger.error("NEWSAPI_KEY is not set. Copy .env.example to .env and set NEWSAPI_KEY, or set the env var.")
        raise SystemExit(1)

    newsapi = NewsApiClient(api_key=newsapi_key)
    producer = create_producer(args.bootstrap_servers)
    dedup = LRUSet(args.dedup_maxsize)

    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received, exiting gracefully...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    page = 1

    logger.info("Starting producer (query=%s) -> %s", args.q, args.topic)

    while running:
        articles = fetch_articles(newsapi, args.q, page=page, page_size=args.page_size)
        if not articles:
            logger.info("No articles returned on page %s", page)
            page = 1
            time.sleep(args.poll_interval)
            continue

        new_count = 0
        for art in articles:
            # create a dedupe key - prefer url, fallback to title+publishedAt
            key = art.get("url") or f"{art.get('title')}|{art.get('publishedAt')}"
            if not key:
                continue
            if key in dedup:
                continue
            dedup.add(key)
            payload = {
                "source": art.get("source"),
                "author": art.get("author"),
                "title": art.get("title"),
                "description": art.get("description"),
                "url": art.get("url"),
                "publishedAt": art.get("publishedAt"),
                "content": art.get("content"),
                "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "query": args.q,
            }
            try:
                producer.produce(args.topic, json.dumps(payload), callback=delivery_report)
                new_count += 1
            except BufferError:
                logger.warning("Local producer queue is full, calling poll to flush...")
                producer.poll(1)
                try:
                    producer.produce(args.topic, json.dumps(payload), callback=delivery_report)
                    new_count += 1
                except Exception:
                    logger.exception("Failed to produce message after flushing")
            except Exception:
                logger.exception("Unexpected error producing message")

        # Force delivery callbacks and flush small amount
        producer.poll(0)
        logger.info("Fetched page %s: %s articles, %s new sent", page, len(articles), new_count)

        # NewsAPI paginates, iterate pages until empty then start over
        page += 1
        if page > 5:
            page = 1

        # sleep between polls
        for _ in range(int(args.poll_interval)):
            if not running:
                break
            time.sleep(1)

    logger.info("Flushing producer... (waiting up to 10s)")
    producer.flush(10)
    logger.info("Producer stopped")


if __name__ == "__main__":
    main()
