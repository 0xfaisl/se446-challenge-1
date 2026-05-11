# ingester.py — T1 (10 pts)
# Pulls headlines from 4+ RSS feeds every 60s, writes JSON-lines into data/incoming/
#
# HOW IT WORKS:
#   - feedparser.parse(url) returns a parsed feed object
#   - feed.entries is a list of articles; each entry has .title, .link, .published
#   - We write one .json file per tick, with one JSON object per line (JSONL format)
#   - Spark Structured Streaming will watch data/incoming/ and pick up new files
#
# RUN: python ingester.py   (keep running in its own terminal)

import os, json, time, feedparser
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "BBC":       "https://feeds.bbci.co.uk/news/world/rss.xml",
    "NYT":       "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "CNN":       "https://rss.cnn.com/rss/edition_world.rss",
    "AlJazeera": "https://www.aljazeera.com/xml/rss/all.xml",
}


def pull_once(tick: int):
    """Pull all feeds once and write a single JSONL batch file."""
    rows = []

    for source, url in FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                raw_ts = entry.get("published", "")
                try:
                    ts = parsedate_to_datetime(raw_ts).isoformat()
                except Exception:
                    ts = datetime.now(timezone.utc).isoformat()
                rows.append({
                    "source": source,
                    "title":  entry.title,
                    "url":    entry.link,
                    "ts":     ts,
                })
        except Exception as e:
            print(f"[WARN] failed to fetch {source}: {e}")

    # Write JSONL file — one JSON object per line
    path = os.path.join(INCOMING, f"batch_{tick}.json")
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"[tick {tick}] wrote {len(rows)} headlines to {path}")


if __name__ == "__main__":
    tick = 0
    while True:
        pull_once(tick)
        tick += 1
        time.sleep(60)  # pull every 60 seconds
