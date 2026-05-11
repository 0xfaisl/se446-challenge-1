# News Pulse — SE446 Big Data Challenge

A real-time news monitoring pipeline: RSS ingestion → Spark Structured Streaming → LLM summarisation → Streamlit dashboard.

## Team

| Name | ID |
|------|----|
|      |    |
|      |    |

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

### LLM Summary Output

```
(Paste the LLM-generated summary here)
```

### Top Keywords

```
(Paste top keywords table here)
```

### Spark Streaming Console Output

```
(Paste any relevant streaming output here)
```

## Reflection (T5)

<!-- Max 100 words. Answer: which step breaks first at 1000x scale, and which Spark feature fixes it? -->

```
(Write your reflection here)
```
