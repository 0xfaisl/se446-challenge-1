# app.py — T4 (25 pts) + T3 (LLM summary, 15 pts)
# Streamlit dashboard that reads from Spark memory sinks and displays charts
#
# HOW IT WORKS:
#   - Streamlit reruns the entire script on every browser interaction / auto-refresh
#   - We connect to the SAME SparkSession that streaming_job.py created (same appName)
#   - spark.sql("SELECT * FROM by_source") reads the memory sink table
#   - We convert to pandas for Streamlit's chart functions
#
# RUN: streamlit run app.py   (in a third terminal, after ingester + streaming_job)

import streamlit as st
import requests, json
from pyspark.sql import SparkSession

# --- Connect to the existing SparkSession ---
spark = SparkSession.builder.appName("NewsPulse").getOrCreate()

st.title("News Pulse — Live Dashboard")

# ========== CHART 1: Bar chart — headlines per source ==========
# TODO: Query the "by_source" memory table and render a bar chart
#
# Steps:
#   1. df_source = spark.sql("SELECT * FROM by_source").toPandas()
#   2. st.subheader("Headlines by Source")
#   3. st.bar_chart(df_source.set_index("source"))
#
# Wrap in try/except in case the table doesn't exist yet (streaming hasn't started)


# ========== CHART 2: Line chart — headlines per hour ==========
# TODO: Query the "by_window" memory table and render a line chart
#
# The window column is a struct with .start and .end fields.
# You'll need to extract the start time for the x-axis:
#   1. df_window = spark.sql("SELECT window.start AS hour, count FROM by_window ORDER BY hour").toPandas()
#   2. st.subheader("Headlines per Hour")
#   3. st.line_chart(df_window.set_index("hour"))


# ========== TABLE: Top keywords ==========
# TODO: Query the "top_words" memory table, limit to top 10
#
#   1. df_words = spark.sql("SELECT word, count FROM top_words ORDER BY count DESC LIMIT 10").toPandas()
#   2. st.subheader("Top 10 Keywords")
#   3. st.table(df_words)


# ========== LLM SUMMARY — T3 (15 pts) ==========
# TODO: Send the top 15 keywords to an LLM and display the summary
#
# Steps:
#   1. Get keywords: df_kw = spark.sql("SELECT word FROM top_words ORDER BY count DESC LIMIT 15").toPandas()
#   2. Build keyword list: keywords = ", ".join(df_kw["word"].tolist())
#   3. Call an LLM API (Claude or OpenAI) with a prompt like:
#
#      PROMPT EXAMPLE:
#        f"You are a news analyst. Given these trending keywords from today's headlines: {keywords}. "
#        f"Write a one-paragraph summary (max 80 words) of what the world is talking about right now. "
#        f"Mention at least three specific storylines by name."
#
#   4. FALLBACK: If the API call fails, show the keywords as-is:
#        summary = f"Trending keywords: {keywords}"
#   5. Display: st.subheader("Thematic Summary")
#              st.info(summary)
#
# --- Claude API example (using requests) ---
# import os
# def get_llm_summary(keywords: str) -> str:
#     try:
#         resp = requests.post(
#             "https://api.anthropic.com/v1/messages",
#             headers={
#                 "x-api-key": os.environ["ANTHROPIC_API_KEY"],
#                 "content-type": "application/json",
#                 "anthropic-version": "2023-06-01",
#             },
#             json={
#                 "model": "claude-sonnet-4-20250514",
#                 "max_tokens": 200,
#                 "messages": [{"role": "user", "content": f"...your prompt with {keywords}..."}],
#             },
#             timeout=15,
#         )
#         return resp.json()["content"][0]["text"]
#     except Exception:
#         return f"Trending keywords: {keywords}"


# ========== T5: REFLECTION (15 pts) ==========
# TODO: Write a one-paragraph reflection (max 100 words) answering:
#   "Which step of your pipeline would break first if input grew 1000x,
#    and which Spark feature would you reach for to fix it?"
#
# Display it at the bottom:
# st.subheader("Reflection")
# st.write("Your reflection here...")
