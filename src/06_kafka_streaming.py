"""
06_kafka_streaming.py
Spark Structured Streaming job that:
  1. Reads JSON edit events from a Kafka topic.
  2. Computes a partial feature vector per event.
  3. Loads the logistic regression weights and scaler from GCS.
  4. Scores each event and writes the prediction to GCS as Parquet.

Args:
    --bootstrap     <kafka bootstrap server>:9092
    --api-key       confluent api key
    --api-secret    confluent api secret
    --topic         wiki-edits
    --weights       gs://.../models/logreg/weights.tmp/part-00000
    --scaler        gs://.../models/logreg/scaler.tmp/part-00000
    --checkpoint    gs://.../checkpoints/streaming/
    --out           gs://.../streaming_predictions/
"""

import argparse
import json
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, LongType)


FEATURE_COLS = ["velocity", "revert_rate", "namespace_diversity",
                "avg_bytes_added", "avg_bytes_removed", "talk_page_ratio",
                "peak_hour_sin", "peak_hour_cos", "weekend_ratio",
                "edit_summary_rate", "minor_edit_ratio",
                "session_count", "first_edit_size"]


# JSON envelope produced by 07_kafka_producer.py
EVENT_SCHEMA = StructType([
    StructField("user", StringType()),
    StructField("title", StringType()),
    StructField("namespace", IntegerType()),
    StructField("ts", LongType()),
    StructField("bytes_diff", LongType()),
    StructField("comment", StringType()),
    StructField("minor", IntegerType()),
    StructField("is_revert", IntegerType()),
])


def load_json(spark, path):
    """Read a Spark text file folder back as one JSON object."""
    rdd = spark.sparkContext.textFile(path)
    text = "\n".join(rdd.collect())
    return json.loads(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--topic", default="wiki-edits")
    parser.add_argument("--weights", required=True)
    parser.add_argument("--scaler", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("WikiFlow-06-streaming")
             .getOrCreate())

    weights = load_json(spark, args.weights)
    scaler = load_json(spark, args.scaler)
    intercept = float(weights["intercept"])
    w_vec = [float(weights["weights"][c]) for c in FEATURE_COLS]
    means = [float(scaler["means"][c]) for c in FEATURE_COLS]
    stds = [float(scaler["stds"][c]) for c in FEATURE_COLS]

    sc = spark.sparkContext
    bc_w = sc.broadcast(w_vec)
    bc_b = sc.broadcast(intercept)
    bc_m = sc.broadcast(means)
    bc_s = sc.broadcast(stds)

    jaas = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username='{args.api_key}' password='{args.api_secret}';")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("kafka.security.protocol", "SASL_SSL")
           .option("kafka.sasl.mechanism", "PLAIN")
           .option("kafka.sasl.jaas.config", jaas)
           .option("subscribe", args.topic)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) as json")
              .select(F.from_json("json", EVENT_SCHEMA).alias("e"))
              .select("e.*"))

    # Per-event partial features. We do not have the editor's history live,
    # so we use single-event proxies. Velocity, namespace diversity and the
    # session-style features are filled with 0 since one event cannot define
    # them; the model still produces a directional risk score driven by the
    # observable features.
    feats = parsed.select(
        "user", "title", "ts",
        F.lit(0.0).alias("velocity"),
        F.col("is_revert").cast("double").alias("revert_rate"),
        F.lit(0.0).alias("namespace_diversity"),
        F.greatest(F.col("bytes_diff"), F.lit(0)).cast("double")
            .alias("avg_bytes_added"),
        F.greatest(-F.col("bytes_diff"), F.lit(0)).cast("double")
            .alias("avg_bytes_removed"),
        ((F.col("namespace") == 1).cast("double")).alias("talk_page_ratio"),
        F.sin(F.lit(2 * math.pi) * (F.from_unixtime("ts").substr(12, 2)
                                    .cast("double")) / F.lit(24.0))
            .alias("peak_hour_sin"),
        F.cos(F.lit(2 * math.pi) * (F.from_unixtime("ts").substr(12, 2)
                                    .cast("double")) / F.lit(24.0))
            .alias("peak_hour_cos"),
        F.lit(0.0).alias("weekend_ratio"),
        ((F.col("comment").isNotNull()) & (F.col("comment") != ""))
            .cast("double").alias("edit_summary_rate"),
        F.col("minor").cast("double").alias("minor_edit_ratio"),
        F.lit(1.0).alias("session_count"),
        F.greatest(F.col("bytes_diff"), F.lit(0)).cast("double")
            .alias("first_edit_size"),
    )

    def score_partition(rows):
        for r in rows:
            x = [r[c] if r[c] is not None else 0.0 for c in FEATURE_COLS]
            xs = [(v - mu) / sd
                  for v, mu, sd in zip(x, bc_m.value, bc_s.value)]
            z = bc_b.value + sum(wi * xi
                                 for wi, xi in zip(bc_w.value, xs))
            # Stable sigmoid
            if z >= 0:
                p = 1.0 / (1.0 + math.exp(-z))
            else:
                ez = math.exp(z)
                p = ez / (1.0 + ez)
            yield (r["user"], r["title"], r["ts"], p)

    # Use mapPartitions via foreachBatch since it streams.
    def write_batch(batch_df, batch_id):
        scored = batch_df.rdd.mapPartitions(score_partition).toDF(
            ["user", "title", "ts", "dropout_risk"])
        (scored.write
            .mode("append")
            .partitionBy()
            .parquet(args.out))

    query = (feats.writeStream
             .option("checkpointLocation", args.checkpoint)
             .foreachBatch(write_batch)
             .trigger(processingTime="60 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
