"""
01_clean_and_partition.py
Read the raw MediaWiki history TSV.bz2 files from GCS, drop bots and odd rows,
keep only main namespace edits and talk page edits, and write Parquet
partitioned by event year and month.

Run on Dataproc:
    gcloud not used. Submit through the Dataproc Jobs UI.
    Args: --raw gs://wikiflow-<id>/raw/ --out gs://wikiflow-<id>/clean/
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, BooleanType)


# Subset of the MediaWiki history schema. The full schema has 70+ columns.
# We declare only what we read so Spark does not waste memory on the rest.
SCHEMA = StructType([
    StructField("wiki_db", StringType()),
    StructField("event_entity", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("event_comment", StringType()),
    StructField("event_user_id", StringType()),
    StructField("event_user_text_historical", StringType()),
    StructField("event_user_text", StringType()),
    StructField("event_user_blocks_historical", StringType()),
    StructField("event_user_blocks", StringType()),
    StructField("event_user_groups_historical", StringType()),
    StructField("event_user_groups", StringType()),
    StructField("event_user_is_bot_by_historical", StringType()),
    StructField("event_user_is_bot_by", StringType()),
    StructField("event_user_is_created_by_self", BooleanType()),
    StructField("event_user_is_created_by_system", BooleanType()),
    StructField("event_user_is_created_by_peer", BooleanType()),
    StructField("event_user_is_anonymous", BooleanType()),
    StructField("event_user_registration_timestamp", StringType()),
    StructField("event_user_creation_timestamp", StringType()),
    StructField("event_user_first_edit_timestamp", StringType()),
    StructField("event_user_revision_count", LongType()),
    StructField("event_user_seconds_since_previous_revision", LongType()),
    StructField("page_id", StringType()),
    StructField("page_title_historical", StringType()),
    StructField("page_title", StringType()),
    StructField("page_namespace_historical", IntegerType()),
    StructField("page_namespace_is_content_historical", BooleanType()),
    StructField("page_namespace", IntegerType()),
    StructField("page_namespace_is_content", BooleanType()),
    StructField("page_is_redirect", BooleanType()),
    StructField("page_is_deleted", BooleanType()),
    StructField("page_creation_timestamp", StringType()),
    StructField("page_first_edit_timestamp", StringType()),
    StructField("page_revision_count", LongType()),
    StructField("page_seconds_since_previous_revision", LongType()),
    StructField("user_id", StringType()),
    StructField("user_text_historical", StringType()),
    StructField("user_text", StringType()),
    StructField("user_blocks_historical", StringType()),
    StructField("user_blocks", StringType()),
    StructField("user_groups_historical", StringType()),
    StructField("user_groups", StringType()),
    StructField("user_is_bot_by_historical", StringType()),
    StructField("user_is_bot_by", StringType()),
    StructField("user_is_created_by_self", BooleanType()),
    StructField("user_is_created_by_system", BooleanType()),
    StructField("user_is_created_by_peer", BooleanType()),
    StructField("user_is_anonymous", BooleanType()),
    StructField("user_registration_timestamp", StringType()),
    StructField("user_creation_timestamp", StringType()),
    StructField("user_first_edit_timestamp", StringType()),
    StructField("revision_id", StringType()),
    StructField("revision_parent_id", StringType()),
    StructField("revision_minor_edit", BooleanType()),
    StructField("revision_deleted_parts", StringType()),
    StructField("revision_deleted_parts_are_suppressed", BooleanType()),
    StructField("revision_text_bytes", LongType()),
    StructField("revision_text_bytes_diff", LongType()),
    StructField("revision_text_sha1", StringType()),
    StructField("revision_content_model", StringType()),
    StructField("revision_content_format", StringType()),
    StructField("revision_is_deleted_by_page_deletion", BooleanType()),
    StructField("revision_deleted_by_page_deletion_timestamp", StringType()),
    StructField("revision_is_identity_reverted", BooleanType()),
    StructField("revision_first_identity_reverting_revision_id", StringType()),
    StructField("revision_seconds_to_identity_revert", LongType()),
    StructField("revision_is_identity_revert", BooleanType()),
    StructField("revision_is_from_before_page_creation", BooleanType()),
    StructField("revision_tags", StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", required=True,
                        help="gs://bucket/raw/ folder with TSV.bz2 files")
    parser.add_argument("--out", required=True,
                        help="gs://bucket/clean/ destination for Parquet")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("WikiFlow-01-clean")
             .config("spark.sql.shuffle.partitions", "400")
             .config("spark.sql.files.maxPartitionBytes", "256m")
             .getOrCreate())

    df = (spark.read
          .option("sep", "\t")
          .option("header", "false")
          .option("quote", "")
          .option("escape", "")
          .schema(SCHEMA)
          .csv(args.raw + "*.tsv.bz2"))

    # Keep only revision events. Drop user-event and page-event rows that
    # share the same file but are not edits.
    df = df.filter(F.col("event_entity") == "revision")

    # Drop edits made by bots. Bot flag column is a comma list of detection
    # methods, empty string means human.
    df = df.filter((F.col("event_user_is_bot_by").isNull()) |
                   (F.col("event_user_is_bot_by") == ""))

    # Drop anonymous edits, we predict registered editor retention.
    df = df.filter(F.col("event_user_is_anonymous") == False)

    # Keep only main (0) and talk (1) namespaces. Other namespaces add noise
    # and are dominated by maintenance bots.
    df = df.filter(F.col("page_namespace").isin(0, 1))

    # Parse timestamp once, derive year and month for partitioning.
    df = df.withColumn("ts", F.to_timestamp("event_timestamp"))
    df = df.withColumn("year", F.year("ts"))
    df = df.withColumn("month", F.month("ts"))

    # Drop the heavy text columns we never use.
    keep = ["wiki_db", "ts", "year", "month",
            "event_user_id", "event_user_text",
            "event_user_registration_timestamp",
            "event_user_first_edit_timestamp",
            "event_user_revision_count",
            "page_id", "page_namespace",
            "revision_id", "revision_minor_edit",
            "revision_text_bytes", "revision_text_bytes_diff",
            "revision_is_identity_revert",
            "revision_is_identity_reverted",
            "event_comment"]
    df = df.select(*keep)

    (df.write
       .mode("overwrite")
       .partitionBy("year", "month")
       .parquet(args.out))

    spark.stop()


if __name__ == "__main__":
    main()
