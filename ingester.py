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

INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

# TODO: Add at least 4 RSS feed URLs. Here are some reliable ones to pick from:
#   - "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"
#   - "https://feeds.bbci.co.uk/news/world/rss.xml"
#   - "https://rss.cnn.com/rss/edition_world.rss"
#   - "https://www.aljazeera.com/xml/rss/all.xml"
#   - "https://feeds.reuters.com/reuters/worldNews"
#   - "https://www.theguardian.com/world/rss"
FEEDS = {
    # "BBC":      "https://feeds.bbci.co.uk/news/world/rss.xml",
    # "NYT":      "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    # "CNN":      "https://rss.cnn.com/rss/edition_world.rss",
    # "AlJazeera":"https://www.aljazeera.com/xml/rss/all.xml",
}


def pull_once(tick: int):
    """Pull all feeds once and write a single JSONL batch file."""
    rows = []

    for source, url in FEEDS.items():
        # TODO: Wrap each feed in try/except so one dead feed doesn't crash everything
        # Steps:
        #   1. feed = feedparser.parse(url)
        #   2. Loop over feed.entries
        #   3. For each entry, build a dict with these 4 keys:
        #        "source": source,                          (string — e.g. "BBC")
        #        "title":  entry.title,                     (string — the headline)
        #        "url":    entry.link,                      (string — article URL)
        #        "ts":     <ISO timestamp string>           (string — see note below)
        #   4. Append the dict to rows
        #
        # TIMESTAMP NOTE:
        #   entry.published exists on most feeds but not all.
        #   Safest approach: use datetime.now(timezone.utc).isoformat()
        #   That way every record has a valid timestamp even if the feed omits it.
        pass

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
