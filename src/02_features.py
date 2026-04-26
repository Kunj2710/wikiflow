"""
02_features.py
For every registered editor, compute a feature vector from their first N
edits and a binary label that is 1 if they made zero edits in the H days
after that window.

Output: one Parquet row per editor with 12 numeric features, the label,
and the editor id.

Args:
    --clean    gs://wikiflow-<id>/clean/
    --out      gs://wikiflow-<id>/features/
    --window   number of first edits used for features (default 10)
    --horizon-days  retention horizon in days (default 180)
"""

import argparse
import math

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--horizon-days", type=int, default=180)
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("WikiFlow-02-features")
             .config("spark.sql.shuffle.partitions", "400")
             .getOrCreate())

    df = spark.read.parquet(args.clean)
    df = df.filter(F.col("event_user_id").isNotNull())

    # Order each editor's edits by time and number them 1..N.
    w = Window.partitionBy("event_user_id").orderBy("ts")
    df = df.withColumn("edit_rank", F.row_number().over(w))

    # First-window edits and post-window edits split.
    first_window = df.filter(F.col("edit_rank") <= args.window)
    post_window = df.filter(F.col("edit_rank") > args.window)

    # Editor's first-edit and last-in-window timestamps.
    win_bounds = (first_window
                  .groupBy("event_user_id")
                  .agg(F.min("ts").alias("first_ts"),
                       F.max("ts").alias("last_window_ts"),
                       F.count("*").alias("window_edits")))

    # Drop editors that never reached the window. We need at least 3 edits
    # to compute meaningful features.
    win_bounds = win_bounds.filter(F.col("window_edits") >= 3)

    # ---- Feature engineering on first-window edits ------------------------

    # 1. velocity: edits per day during the window
    fw = first_window.join(win_bounds, "event_user_id")
    fw = fw.withColumn("window_days",
                       F.greatest(F.lit(1.0),
                                  (F.unix_timestamp("last_window_ts") -
                                   F.unix_timestamp("first_ts")) / 86400.0))
    velocity = (fw.groupBy("event_user_id")
                .agg((F.count("*") / F.first("window_days")).alias("velocity")))

    # 2. revert_rate
    revert_rate = (fw.groupBy("event_user_id")
                   .agg(F.avg(F.col("revision_is_identity_reverted")
                              .cast("double")).alias("revert_rate")))

    # 3. namespace_diversity (Shannon entropy across namespaces)
    ns = (fw.groupBy("event_user_id", "page_namespace")
            .count())
    ns_total = ns.groupBy("event_user_id").agg(F.sum("count").alias("total"))
    ns = ns.join(ns_total, "event_user_id")
    ns = ns.withColumn("p", F.col("count") / F.col("total"))
    ns = ns.withColumn("plog", -F.col("p") * F.log2("p"))
    ns_div = ns.groupBy("event_user_id").agg(
        F.sum("plog").alias("namespace_diversity"))

    # 4 and 5. average bytes added and removed
    bytes_stats = (fw.groupBy("event_user_id")
                   .agg(F.avg(F.when(F.col("revision_text_bytes_diff") > 0,
                                     F.col("revision_text_bytes_diff"))
                              .otherwise(0)).alias("avg_bytes_added"),
                        F.avg(F.when(F.col("revision_text_bytes_diff") < 0,
                                     -F.col("revision_text_bytes_diff"))
                              .otherwise(0)).alias("avg_bytes_removed")))

    # 6. talk_page_ratio
    talk_ratio = (fw.groupBy("event_user_id")
                  .agg(F.avg((F.col("page_namespace") == 1)
                             .cast("double")).alias("talk_page_ratio")))

    # 7. peak hour cyclical encoding
    fw2 = fw.withColumn("hour", F.hour("ts"))
    hour_modes = (fw2.groupBy("event_user_id", "hour")
                  .count()
                  .withColumn("rk",
                              F.row_number().over(
                                  Window.partitionBy("event_user_id")
                                  .orderBy(F.desc("count"))))
                  .filter(F.col("rk") == 1)
                  .select("event_user_id", "hour"))
    hour_modes = hour_modes.withColumn(
        "peak_hour_sin", F.sin(2 * math.pi * F.col("hour") / 24.0))
    hour_modes = hour_modes.withColumn(
        "peak_hour_cos", F.cos(2 * math.pi * F.col("hour") / 24.0))
    hour_modes = hour_modes.select(
        "event_user_id", "peak_hour_sin", "peak_hour_cos")

    # 8. weekend_ratio
    weekend = (fw.withColumn("dow", F.dayofweek("ts"))
               .groupBy("event_user_id")
               .agg(F.avg(F.when(F.col("dow").isin(1, 7), 1.0)
                          .otherwise(0.0)).alias("weekend_ratio")))

    # 9. edit_summary_rate
    summary = (fw.groupBy("event_user_id")
               .agg(F.avg(F.when((F.col("event_comment").isNotNull()) &
                                 (F.col("event_comment") != ""), 1.0)
                          .otherwise(0.0)).alias("edit_summary_rate")))

    # 10. minor_edit_ratio
    minor = (fw.groupBy("event_user_id")
             .agg(F.avg(F.col("revision_minor_edit").cast("double"))
                  .alias("minor_edit_ratio")))

    # 11. session_count: gap of more than 30 minutes starts a new session
    fw3 = fw.withColumn(
        "prev_ts",
        F.lag("ts").over(Window.partitionBy("event_user_id").orderBy("ts")))
    fw3 = fw3.withColumn(
        "gap_min",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 60.0)
    fw3 = fw3.withColumn("new_session",
                         F.when((F.col("gap_min").isNull()) |
                                (F.col("gap_min") > 30), 1).otherwise(0))
    sessions = (fw3.groupBy("event_user_id")
                .agg(F.sum("new_session").alias("session_count")))

    # 12. first_edit_size
    first_size = (fw.filter(F.col("edit_rank") == 1)
                  .select("event_user_id",
                          F.coalesce(F.col("revision_text_bytes"),
                                     F.lit(0)).alias("first_edit_size")))

    # ---- Label: dropout = 1 if zero post-window edits within horizon ------

    horizon = args.horizon_days * 86400
    pw = post_window.join(win_bounds, "event_user_id")
    pw = pw.withColumn(
        "within_horizon",
        F.when((F.unix_timestamp("ts") -
                F.unix_timestamp("last_window_ts")) <= horizon, 1)
        .otherwise(0))
    label = (pw.groupBy("event_user_id")
             .agg(F.max("within_horizon").alias("kept")))
    # Editors absent from post_window get kept = 0 (full dropout) via outer
    # join below.

    # ---- Stitch all features together ------------------------------------

    feats = (win_bounds
             .join(velocity, "event_user_id", "left")
             .join(revert_rate, "event_user_id", "left")
             .join(ns_div, "event_user_id", "left")
             .join(bytes_stats, "event_user_id", "left")
             .join(talk_ratio, "event_user_id", "left")
             .join(hour_modes, "event_user_id", "left")
             .join(weekend, "event_user_id", "left")
             .join(summary, "event_user_id", "left")
             .join(minor, "event_user_id", "left")
             .join(sessions, "event_user_id", "left")
             .join(first_size, "event_user_id", "left")
             .join(label, "event_user_id", "left"))

    feats = feats.withColumn("dropout",
                             F.when((F.col("kept") == 1), 0).otherwise(1))

    feature_cols = ["velocity", "revert_rate", "namespace_diversity",
                    "avg_bytes_added", "avg_bytes_removed", "talk_page_ratio",
                    "peak_hour_sin", "peak_hour_cos", "weekend_ratio",
                    "edit_summary_rate", "minor_edit_ratio",
                    "session_count", "first_edit_size"]

    for c in feature_cols:
        feats = feats.withColumn(c, F.coalesce(F.col(c), F.lit(0.0))
                                 .cast("double"))

    out = feats.select("event_user_id", "dropout", *feature_cols)
    out.write.mode("overwrite").parquet(args.out)

    spark.stop()


if __name__ == "__main__":
    main()
