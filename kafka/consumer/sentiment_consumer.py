"""
read hn stories from kafka, runs VADER sentiment analysis, save results to duckDB
"""

import os
import sys
import json
from pathlib import Path
import time
from datetime import datetime, timezone
import duckdb
from confluent_kafka import Consumer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "reddit-posts")
DB_PATH       = os.getenv("DB_PATH", str(PROJECT_ROOT / "reddit_sentiment.db"))

analyzer = SentimentIntensityAnalyzer()

def create_kafka_consumer():
    config = {
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "sentiment-consumer-group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def create_database():
    """Create DuckDB database and raw table."""
    con = duckdb.connect(DB_PATH)

    # Check if raw schema exists, create if not
    schemas = [row[0] for row in con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()]
    if 'raw' not in schemas:
        con.execute("CREATE SCHEMA raw")

    con.execute("""
                CREATE TABLE IF NOT EXISTS raw.hn_posts
                (
                    post_id VARCHAR PRIMARY KEY,
                    category VARCHAR,
                    title VARCHAR,
                    selftext VARCHAR,
                    author VARCHAR,
                    score INTEGER,
                    num_comments INTEGER,
                    url VARCHAR,
                    created_utc TIMESTAMP,
                    fetched_at TIMESTAMP,
                    is_self BOOLEAN,
                    sentiment_pos DOUBLE,
                    sentiment_neg DOUBLE,
                    sentiment_neu DOUBLE,
                    sentiment_compound DOUBLE,
                    sentiment_label VARCHAR,
                    processed_at TIMESTAMP
                )
                """)
    con.close()
    print(f"Database ready: {DB_PATH}")


def analyze_sentiment(title, selftext):
    """
    run VADER sentiment on title + body text
    returns dict with pos, neg, neu, compound, label
    """
    full_text = f"{title}. {selftext}" if selftext else title
    scores = analyzer.polarity_scores(full_text)

    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return {
        "pos": scores["pos"],
        "neg": scores["neg"],
        "neu": scores["neu"],
        "compound": compound,
        "label": label,
    }

def save_to_duckdb(post, sentiment):
    """Insert processed post into DuckDB."""
    con = duckdb.connect(DB_PATH)
    con.execute("""
        INSERT OR REPLACE INTO raw.hn_posts VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
    """, [
        post["post_id"],
        post["category"],
        post["title"],
        post.get("selftext", ""),
        post["author"],
        post["score"],
        post["num_comments"],
        post["url"],
        post["created_utc"],
        post["fetched_at"],
        post.get("is_self", False),
        sentiment["pos"],
        sentiment["neg"],
        sentiment["neu"],
        sentiment["compound"],
        sentiment["label"],
        datetime.now(timezone.utc).isoformat(),
    ])
    con.close()

def run_consumer():
    """read from kafka -> analyze -> save to duckdb"""
    print("Waiting 20s for Kafka and producer to start...")
    time.sleep(20)
    create_database()
    consumer = create_kafka_consumer()

    print("=" * 55)
    print("Sentiment Consumer")
    print(f"  Kafka topic : {KAFKA_TOPIC}")
    print(f"  Database    : {DB_PATH}")
    print("=" * 55)
    print("Consuming... Press Ctrl+C to stop.\n")

    count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                post = json.loads(msg.value().decode("utf-8"))
            except json.decoder.JSONDecodeError:
                print("Skipping malformed message")
                continue

            # analyze sentiment
            sentiment = analyze_sentiment(
                post.get("title", ""),
                post.get("selftext", "")
            )

            save_to_duckdb(post, sentiment)
            count += 1
            label = sentiment["label"].upper()
            compound = sentiment["compound"]
            print(
                f"[{count}] [{label:>8}] ({compound:+.2f}) "
                f"{post['category']}: {post['title'][:65]}"
            )

    except KeyboardInterrupt:
        print(f"\nStopping... Processed {count} messages.")
        consumer.close()
        print("Done.")

if __name__ == "__main__":
    run_consumer()


