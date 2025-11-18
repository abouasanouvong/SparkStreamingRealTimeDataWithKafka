#!/usr/bin/env python3
"""
PySpark Structured Streaming job:
- Reads messages from Kafka topic `topic1` (value expected to be JSON produced by the producer script).
- Extracts text fields and runs NER (Hugging Face Transformers) using a Pandas UDF.
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
- Requires pyspark, pandas, torch, and transformers.
- The default NER model is 'dslim/bert-base-NER'.
"""

import os
import argparse
import logging
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, explode, pandas_udf, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pandas as pd

# Hugging Face Transformers is used for NER
try:
    from transformers import pipeline
    _TRANSFORMERS_AVAILABLE = True
except ImportError:
    pipeline = None
    _TRANSFORMERS_AVAILABLE = False

logger = logging.getLogger("spark_entity_counter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Lazy-loaded Hugging Face NER pipeline
_ner_pipeline = None


def get_ner_pipeline():
    """Lazily loads and returns the Hugging Face NER pipeline."""
    global _ner_pipeline
    if _ner_pipeline is None:
        if not _TRANSFORMERS_AVAILABLE:
            raise RuntimeError("Transformers library is not available")
        try:
            # Using a popular NER model. It groups entities by default.
            _ner_pipeline = pipeline("ner", model="dslim/bert-base-NER", grouped_entities=True, device="cpu")
        except Exception as e:
            logger.error("Failed to load Hugging Face NER pipeline: %s", e)
            raise
    return _ner_pipeline


def extract_entities_text(text: str) -> List[str]:
    """Return list of entity strings from the provided text using a Hugging Face NER model."""
    if not text:
        return []
    text = str(text)
    try:
        ner_pipeline = get_ner_pipeline()
        entities = ner_pipeline(text)
        # The pipeline with grouped_entities=True returns a list of dicts
        # e.g., [{'entity_group': 'PER', 'score': 0.99, 'word': 'John Doe'}]
        # We filter for common entity types.
        filtered_ents = [
            entity['word'] for entity in entities
            if entity['entity_group'] in {"PER", "ORG", "LOC", "MISC"}
        ]
        return filtered_ents
    except Exception as e:
        logger.warning("Hugging Face NER extraction failed: %s", e)
        return []


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