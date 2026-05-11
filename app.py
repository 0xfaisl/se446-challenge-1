# app.py — T4 (25 pts) + T3 (LLM summary, 15 pts)
# Streamlit dashboard that reads from Spark memory sinks and displays charts
#
# Memory sinks live inside one JVM, so the streaming queries must run
# in the same process as the dashboard. We start them once using
# Streamlit's session_state so they survive reruns.
#
# RUN: streamlit run app.py   (ingester.py must be running in another terminal)

import streamlit as st
import requests, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

st.set_page_config(page_title="News Pulse", layout="wide")

# --- SparkSession (one per process) ---
spark = SparkSession.builder.appName("NewsPulse").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- Start streaming queries once ---
if "streaming_started" not in st.session_state:
    schema = StructType([
        StructField("source", StringType()),
        StructField("title",  StringType()),
        StructField("url",    StringType()),
        StructField("ts",     TimestampType()),
    ])

    stream = spark.readStream.schema(schema).json("data/incoming")

    # Query 1: by_source
    stream.groupBy("source").count() \
        .writeStream.outputMode("complete") \
        .format("memory").queryName("by_source").start()

    # Query 2: by_window
    stream.withWatermark("ts", "24 hours") \
        .groupBy(F.window("ts", "1 hour")).count() \
        .writeStream.outputMode("complete") \
        .format("memory").queryName("by_window").start()

    # Query 3: top_words
    STOP_WORDS = {
        "the","a","an","in","on","at","to","for","of","and","is","it","by",
        "with","from","as","or","that","this","are","was","be","has","have",
        "not","but","its","all","been","can","will","no","do","if","up","out",
        "about","into","over","after","they","who","what","when","how","more",
        "new","also","just","than","very","so","says","said","would","could",
    }

    words = (stream
             .select(F.explode(
                 F.regexp_extract_all(F.col("title"), F.lit(r"([A-Za-z]+)"))
             ).alias("word"))
             .select(F.lower(F.col("word")).alias("word"))
             .filter(~F.col("word").isin(STOP_WORDS))
             .filter(F.length(F.col("word")) > 2))

    words.groupBy("word").count() \
        .writeStream.outputMode("complete") \
        .format("memory").queryName("top_words").start()

    time.sleep(5)
    st.session_state["streaming_started"] = True


# ========== DASHBOARD ==========
st.title("News Pulse — Live Dashboard")

# --- Bar chart: headlines per source ---
try:
    df_source = spark.sql("SELECT * FROM by_source ORDER BY count DESC").toPandas()
    st.subheader("Headlines by Source")
    st.bar_chart(df_source.set_index("source"))
except Exception:
    st.info("Waiting for streaming data (by_source)...")

# --- Line chart: headlines per hour ---
try:
    df_window = spark.sql(
        "SELECT window.start AS hour, count FROM by_window ORDER BY hour"
    ).toPandas()
    st.subheader("Headlines per Hour")
    st.line_chart(df_window.set_index("hour"))
except Exception:
    st.info("Waiting for streaming data (by_window)...")

# --- Table: top 10 keywords ---
try:
    df_words = spark.sql(
        "SELECT word, count FROM top_words ORDER BY count DESC LIMIT 10"
    ).toPandas()
    st.subheader("Top 10 Keywords")
    st.table(df_words)
except Exception:
    st.info("Waiting for streaming data (top_words)...")


# ========== LLM SUMMARY ==========
def get_llm_summary(keywords: str) -> str:
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 200,
                "messages": [{"role": "user", "content":
                    f"You are a news analyst. Given these trending keywords from today's "
                    f"headlines: {keywords}. Write a one-paragraph summary (max 80 words) "
                    f"of what the world is talking about right now. Mention at least three "
                    f"specific storylines by name."
                }],
            },
            timeout=15,
        )
        return resp.json()["content"][0]["text"]
    except Exception:
        return f"Trending keywords: {keywords}"

try:
    df_kw = spark.sql(
        "SELECT word FROM top_words ORDER BY count DESC LIMIT 15"
    ).toPandas()
    keywords = ", ".join(df_kw["word"].tolist())
    summary = get_llm_summary(keywords)
    st.subheader("Thematic Summary")
    st.info(summary)
except Exception:
    st.info("Waiting for keyword data for LLM summary...")


# ========== REFLECTION ==========
st.subheader("Reflection")
st.write(
    "At 1000× scale, the single-machine ingester would break first — feedparser "
    "runs sequentially and cannot keep up with thousands of feeds. The Spark "
    "streaming layer would also hit memory limits on the driver since we use "
    "memory sinks. To fix this, we would distribute ingestion across a cluster, "
    "replace memory sinks with a distributed sink like Parquet on HDFS, and "
    "leverage Spark's native partitioning and checkpointing for fault tolerance."
)
