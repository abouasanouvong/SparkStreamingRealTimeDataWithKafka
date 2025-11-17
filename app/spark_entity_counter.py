#!/usr/bin/env python3
"""
PySpark Structured Streaming job:
- Reads messages from Kafka topic `topic1` (value expected to be JSON produced by the producer script).
- Extracts text fields and runs NER (spaCy if available, otherwise a simple regex fallback) using a Pandas UDF.
- Keeps a running count of named entities using Spark's stateful aggregation.
- On every processing trigger, publishes the complete set of entity counts to Kafka topic `topic2`.

Usage (example):
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
    app/spark_entity_counter.py \
    --bootstrap-servers localhost:9092 \
    --input-topic topic1 \
    --output-topic topic2 \
    --trigger 30s

Notes:
- Requires pyspark and pandas. spaCy is optional for better NER.
- For spaCy, install `spacy` and download model `python -m spacy download en_core_web_sm`.
"""

import os
import argparse
import logging
import re
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, explode, pandas_udf, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pandas as pd

# spaCy is optional; if not available we fall back to a simple regex-based extractor
try:
    import spacy
    _SPACY_AVAILABLE = True
except ImportError:
    spacy = None
    _SPACY_AVAILABLE = False

logger = logging.getLogger("spark_entity_counter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Lazy-loaded spaCy model
_nlp = None


def get_spacy_nlp():
    """Lazily loads and returns the spaCy model."""
    global _nlp
    if _nlp is None:
        if not _SPACY_AVAILABLE:
            raise RuntimeError("spaCy is not available")
        # Try to load the small english model; user must have downloaded it
        try:
            _nlp = spacy.load("en_core_web_sm")
        except Exception as e:
            logger.error("Failed to load spaCy model 'en_core_web_sm': %s", e)
            raise
    return _nlp


# Simple regex fallback for capturing sequences of capitalized words (very basic)
_ENTITY_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b")

def extract_entities_text(text: str) -> List[str]:
    """Return list of entity strings from the provided text using spaCy if available,
    otherwise use a simple regex-based extraction."""
    if not text:
        return []
    text = str(text)
    if _SPACY_AVAILABLE:
        try:
            nlp = get_spacy_nlp()
            doc = nlp(text)
            ents = [ent.text for ent in doc.ents if ent.label_ in {"PERSON", "ORG", "GPE", "LOC", "PRODUCT", "EVENT"}]
            return ents
        except Exception as e:
            logger.warning("spaCy extraction failed, falling back to regex: %s", e)
    # fallback
    matches = _ENTITY_RE.findall(text)
    # filter out short / common words
    filtered = [m for m in matches if len(m) > 2]
    return filtered


@pandas_udf(ArrayType(StringType()))
def extract_entities_udf(texts: pd.Series) -> pd.Series:
    """Pandas UDF to extract entities from a series of text."""
    return texts.apply(extract_entities_text)


def main():
    parser = argparse.ArgumentParser(description="PySpark streaming NER entity counter")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-topic", default=os.getenv("INPUT_TOPIC", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("OUTPUT_TOPIC", "topic2"))
    parser.add_argument("--trigger", default=os.getenv("TRIGGER", "30 seconds"), help="Processing trigger interval, e.g. '30 seconds'")
    parser.add_argument("--app-name", default="spark_entity_counter")
    parser.add_argument("--bootstrap-opts", default=os.getenv("SPARK_OPTS", ""), help="Extra Spark config string (unused)")
    args = parser.parse_args()

    # Build SparkSession
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Define schema expected in Kafka value (produced previously by the News producer)
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("author", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("publishedAt", StringType(), True),
        StructField("content", StringType(), True),
        StructField("fetchedAt", StringType(), True),
        StructField("query", StringType(), True),
    ])

    kafka_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", "latest")
            .load()
    )

    # Parse JSON value
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed = parsed.select(from_json(col("json_str"), schema).alias("data"))
    # Build a text field from title/description/content
    parsed = parsed.select(
        concat_ws(" ", col("data.title"), col("data.description"), col("data.content")).alias("text")
    )

    # Extract entities using the Pandas UDF and explode into separate rows
    entities = parsed.withColumn("entities", extract_entities_udf(col("text")))
    exploded_entities = entities.select(explode(col("entities")).alias("entity"))

    # Perform stateful aggregation to get running counts
    entity_counts = exploded_entities.groupBy("entity").count()

    # Prepare the output DataFrame for Kafka. Each row will be a JSON message.
    output_df = entity_counts.withColumn("timestamp", current_timestamp()).select(
        to_json(struct(col("entity"), col("count"), col("timestamp"))).alias("value")
    )

    # Write the aggregated counts to the output Kafka topic
    query = (
        output_df.writeStream
            .outputMode("complete")
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.output_topic)
            .option("checkpointLocation", "/tmp/spark_entity_counter_checkpoint")
            .trigger(processingTime=args.trigger)
            .start()
    )

    logger.info("Started stream: reading from %s -> aggregating -> writing to %s every %s", args.input_topic, args.output_topic, args.trigger)
    query.awaitTermination()


if __name__ == "__main__":
    main()