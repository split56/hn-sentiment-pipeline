import os
import sys
import json
import time
import urllib.request
from pathlib import Path
from datetime import datetime,timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

HN_BASE_URL    = os.getenv("HN_BASE_URL", "https://hacker-news.firebaseio.com/v0")
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "hn-posts")
CATEGORIES     = os.getenv("HN_CATEGORIES", "topstories,newstories,beststories").split(",")
POLL_INTERVAL  = 60     # seconds between fetches
STORIES_PER_CAT = 30    # stories per category per fetch

def fetch_json(url):
    """Fetch Json from url"""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "HN-Sentiment-Pipeline/1.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        print(f"  Error fetching {url}: {e}")
        return None

def create_kafka_producer():
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "hn-producer",
    })
def delivery_callback(err, msg):
    if err:
        print(f"  Delivery failed: {err}")

def story_to_message(story,category):
    """Convert a story to a message"""
    if story is None or story.get("type") != "story":
        return None

    created_ts = datetime.fromtimestamp(
        story.get("time",0), tz=timezone.utc
    ).isoformat()

    return {
        "post_id": str(story.get("id", "")),
        "category": category,
        "title": story.get("title", ""),
        "selftext": story.get("text", ""),  # body text for Ask HN / Show HN
        "author": story.get("by", "[deleted]"),
        "score": story.get("score", 0),
        "num_comments": story.get("descendants", 0),
        "url": story.get("url", f"https://news.ycombinator.com/item?id={story.get('id')}"),
        "created_utc": created_ts,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "is_self": story.get("text") is not None,
    }

def run_producer():
    """fetch Hn stories and send to kafka"""
    print("Waiting 15s for Kafka to start...")
    time.sleep(15)

    producer = create_kafka_producer()
    seen_ids = set()

    print("=" * 55)
    print("Hacker News Kafka Producer")
    print(f"  Categories  : {CATEGORIES}")
    print(f"  Kafka topic : {KAFKA_TOPIC}")
    print(f"  Poll every  : {POLL_INTERVAL}s")
    print(f"  Stories/cat : {STORIES_PER_CAT}")
    print("=" * 55)
    print("Streaming... Press Ctrl+C to stop.\n")

    try:
        while True:
            new_count = 0
            for category in CATEGORIES:
                print(f"\nFetching stories for {category}...")

                # get story ids
                ids = fetch_json(f"{HN_BASE_URL}/{category}.json")
                if not ids:
                    print(f"  No stories for {category}")
                    continue

                # fetch story's details
                for story_id in ids[:STORIES_PER_CAT]:
                    if story_id in seen_ids:
                        continue

                    seen_ids.add(story_id)

                    story = fetch_json(f"{HN_BASE_URL}/item/{story_id}.json")
                    message = story_to_message(story,category)
                    if message is None:
                        continue

                    # send to kafka
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key = message["post_id"].encode("utf-8"),
                        value = json.dumps(message).encode("utf-8"),
                        callback=delivery_callback,
                    )

                    new_count += 1
                    print(
                        f"  [{category}] "
                        f"(score:{message['score']} "
                        f"comments:{message['num_comments']}) "
                        f"{message['title'][:70]}"
                    )

                    time.sleep(0.2)

                producer.flush()

            # prevent memory leak
            if len(seen_ids) > 10000:
                seen_ids = set(list(seen_ids)[-5000:])
            print(f"\n--- {new_count} new stories. Waiting {POLL_INTERVAL}s ---\n")
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.flush()
        print("Done.")

if __name__ == "__main__":
    run_producer()