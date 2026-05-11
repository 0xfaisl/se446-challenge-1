# News Pulse — SE446 Big Data Challenge

A real-time news monitoring pipeline: RSS ingestion → Spark Structured Streaming → LLM summarisation → Streamlit dashboard.

## Team

| Name              | ID     |
|-------------------|--------|
| Faisal Hajj Khalil | 230023 |
| Tanzim Alam        | 220693 |

## Setup

```bash
# Java 11 or 17 required
java -version

# Install dependencies
pip install pyspark==3.5.0 feedparser pandas streamlit requests

# Verify
python -c "import pyspark, feedparser; print('ok')"

# Set your LLM API key
export ANTHROPIC_API_KEY=sk-...   # or OPENAI_API_KEY=sk-...
```

## Run

Open three terminals:

```bash
# Terminal 1 — Ingester (pulls RSS every 60s)
python ingester.py

# Terminal 2 — Spark Structured Streaming
python streaming_job.py

# Terminal 3 — Streamlit Dashboard
streamlit run app.py
```

## Architecture

```
RSS Feeds (4+)
     │
     ▼
ingester.py ──writes JSONL──▶ data/incoming/
                                    │
                                    ▼
                         streaming_job.py (Spark readStream)
                            ├── by_source  (count per feed)
                            ├── by_window  (count per hour)
                            └── top_words  (top 10 keywords)
                                    │
                                    ▼
                              app.py (Streamlit)
                            ├── Bar chart (sources)
                            ├── Line chart (hourly volume)
                            ├── Top keywords table
                            └── LLM thematic summary
```

## Output

### Dashboard Screenshot

<!-- Paste screenshot here -->

### Ingester Output

```
[OK] BBC: 38 headlines
[OK] NYT: 55 headlines
[OK] CNN: 0 headlines
[OK] AlJazeera: 25 headlines

Total: 118 headlines written to data/incoming/batch_fresh.json
```

### Headlines by Source

```
+---------+-----+
|source   |count|
+---------+-----+
|NYT      |55   |
|BBC      |38   |
|AlJazeera|25   |
+---------+-----+
```

### Headlines by Hour (Tumbling Window)

```
+-------------------+-----+
|hour               |count|
+-------------------+-----+
|2026-05-07 22:00:00|1    |
|2026-05-08 16:00:00|1    |
|2026-05-08 20:00:00|1    |
|2026-05-08 21:00:00|1    |
|2026-05-08 22:00:00|3    |
|2026-05-09 00:00:00|1    |
|2026-05-09 02:00:00|5    |
|2026-05-09 04:00:00|1    |
|2026-05-09 07:00:00|1    |
|2026-05-09 12:00:00|3    |
|2026-05-09 13:00:00|2    |
|2026-05-09 14:00:00|1    |
|2026-05-09 15:00:00|3    |
|2026-05-09 18:00:00|3    |
|2026-05-09 19:00:00|1    |
|2026-05-09 20:00:00|2    |
|2026-05-10 00:00:00|1    |
|2026-05-10 02:00:00|4    |
|2026-05-10 03:00:00|1    |
|2026-05-10 05:00:00|1    |
|2026-05-10 06:00:00|2    |
|2026-05-10 07:00:00|1    |
|2026-05-10 12:00:00|1    |
|2026-05-10 13:00:00|3    |
|2026-05-10 14:00:00|3    |
|2026-05-10 15:00:00|1    |
|2026-05-10 16:00:00|3    |
|2026-05-10 17:00:00|2    |
|2026-05-10 18:00:00|1    |
|2026-05-10 19:00:00|1    |
|2026-05-10 22:00:00|4    |
|2026-05-10 23:00:00|1    |
|2026-05-11 00:00:00|3    |
|2026-05-11 01:00:00|1    |
|2026-05-11 02:00:00|1    |
|2026-05-11 04:00:00|1    |
|2026-05-11 05:00:00|2    |
|2026-05-11 06:00:00|3    |
|2026-05-11 07:00:00|6    |
|2026-05-11 08:00:00|1    |
|2026-05-11 09:00:00|3    |
|2026-05-11 10:00:00|7    |
|2026-05-11 11:00:00|7    |
|2026-05-11 12:00:00|10   |
|2026-05-11 13:00:00|10   |
|2026-05-11 14:00:00|3    |
+-------------------+-----+
```

### Top 10 Keywords

```
+----------+-----+
|word      |count|
+----------+-----+
|iran      |15   |
|trump     |14   |
|war       |12   |
|hantavirus|11   |
|ship      |9    |
|china     |6    |
|cruise    |6    |
|why       |5    |
|end       |5    |
|one       |5    |
+----------+-----+
```

### Top 15 Keywords (LLM Prompt Input)

```
iran, trump, war, hantavirus, ship, china, cruise, end, thailand, ukraine, outbreak, why, bbc, eurovision, one
```

### LLM Summary Output

```
Trending keywords: iran, trump, war, hantavirus, ship, china, cruise, end, thailand, ukraine, outbreak, why, bbc, eurovision, one
```

> Note: Set `export ANTHROPIC_API_KEY=sk-...` to get a full narrative summary.
> Without the key, the dashboard falls back to keyword-only mode (required by T3).

## Reflection (T5)

At 1000x scale, the single-machine ingester would break first feedparser runs sequentially and
cannot keep up with thousands of feeds. The Spark streaming layer would also hit memory limits
on the driver since we use memory sinks. To fix this, we would distribute ingestion across a
cluster, replace memory sinks with a distributed sink like Parquet on HDFS, and leverage Spark's
native partitioning and checkpointing for fault tolerance.
