# streaming_job.py — T2 (25 pts)
# Spark Structured Streaming: watches data/incoming/ and produces 3 aggregations
#
# HOW IT WORKS:
#   - spark.readStream.schema(schema).json("data/incoming") watches the folder
#   - Every time the ingester drops a new .json file, Spark treats it as a micro-batch
#   - We create 3 streaming queries, each writing to a "memory" sink (an in-memory table)
#   - Streamlit (app.py) connects to the SAME SparkSession and reads those memory tables
#
# RUN: python streaming_job.py   (keep running in its own terminal)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --- Step 1: Create SparkSession in local mode ---
spark = (SparkSession.builder
         .appName("NewsPulse")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# --- Step 2: Define the schema matching what ingester.py writes ---
# TODO: Fill in the 4 fields. Each JSON line from the ingester has:
#   source (string), title (string), url (string), ts (timestamp)
#
# Example:
#   schema = StructType([
#       StructField("source", StringType()),
#       StructField("title",  StringType()),
#       StructField("url",    StringType()),
#       StructField("ts",     TimestampType()),
#   ])
schema = StructType([
    StructField("source", StringType()),
    StructField("title",  StringType()),
    StructField("url",    StringType()),
    StructField("ts",     TimestampType()),
])

# --- Step 3: Create the streaming source ---
stream = spark.readStream.schema(schema).json("data/incoming")


# ========== QUERY 1: by_source (headline count per source) ==========
# This one is given to you in the PDF. It groups by source and counts.
# outputMode("complete") means every micro-batch re-emits the full table.
q_src = (stream
         .groupBy("source").count()
         .writeStream
         .outputMode("complete")
         .format("memory")
         .queryName("by_source")
         .start())


# ========== QUERY 2: by_window (headline count per 1-hour tumbling window) ==========
q_win = (stream
         .withWatermark("ts", "24 hours")
         .groupBy(F.window("ts", "1 hour"))
         .count()
         .writeStream
         .outputMode("complete")
         .format("memory")
         .queryName("by_window")
         .start())


# ========== QUERY 3: top_words (top 10 keywords from all headlines) ==========
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

q_words = (words
           .groupBy("word").count()
           .writeStream
           .outputMode("complete")
           .format("memory")
           .queryName("top_words")
           .start())


# --- Keep the streaming queries alive ---
spark.streams.awaitAnyTermination()
