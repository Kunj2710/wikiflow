"""
05_elephas_dnn.py
Distributed Keras deep learning over Spark with Elephas. The driver builds a
Keras Sequential network, wraps it in elephas.SparkModel, and trains over the
RDD of feature vectors. Workers do the heavy lifting in parallel.

Args:
    --features  gs://.../features/
    --out       gs://.../models/dnn/
    --metrics   gs://.../metrics/dnn.json
    --epochs    default 15
    --batch     default 256
"""

import argparse
import json
import math
import time

import numpy as np

from pyspark.sql import SparkSession


FEATURE_COLS = ["velocity", "revert_rate", "namespace_diversity",
                "avg_bytes_added", "avg_bytes_removed", "talk_page_ratio",
                "peak_hour_sin", "peak_hour_cos", "weekend_ratio",
                "edit_summary_rate", "minor_edit_ratio",
                "session_count", "first_edit_size"]


def build_model(input_dim):
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Dense, Dropout
    m = Sequential()
    m.add(Dense(64, activation="relu", input_dim=input_dim))
    m.add(Dropout(0.2))
    m.add(Dense(32, activation="relu"))
    m.add(Dense(1, activation="sigmoid"))
    m.compile(optimizer="adam",
              loss="binary_crossentropy",
              metrics=["accuracy"])
    return m


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--epochs", type=int, default=15)
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("WikiFlow-05-elephas-dnn")
             .getOrCreate())
    sc = spark.sparkContext

    df = spark.read.parquet(args.features)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=args.seed)

    def to_pair(r):
        return ([float(r[c]) for c in FEATURE_COLS], float(r["dropout"]))

    train_pairs = train_df.rdd.map(to_pair).cache()
    test_pairs = test_df.rdd.map(to_pair).cache()

    # Feature standardization stats from training set.
    n = train_pairs.count()
    sums = train_pairs.map(lambda x: x[0]).reduce(
        lambda a, b: [x + y for x, y in zip(a, b)])
    means = [s / n for s in sums]
    sqs = train_pairs.map(
        lambda x: [(v - m) ** 2 for v, m in zip(x[0], means)]).reduce(
        lambda a, b: [x + y for x, y in zip(a, b)])
    stds = [math.sqrt(s / n) if s > 0 else 1.0 for s in sqs]

    bc_m = sc.broadcast(means)
    bc_s = sc.broadcast(stds)

    def scale(p):
        x, y = p
        return ([(v - mu) / sd
                 for v, mu, sd in zip(x, bc_m.value, bc_s.value)], y)

    train_pairs = train_pairs.map(scale).cache()
    test_pairs = test_pairs.map(scale).cache()

    # Elephas needs an RDD of (np.array(features), np.array(label)).
    train_rdd = train_pairs.map(
        lambda p: (np.array(p[0], dtype=np.float32),
                   np.array([p[1]], dtype=np.float32)))

    from elephas.spark_model import SparkModel

    model = build_model(len(FEATURE_COLS))
    spark_model = SparkModel(
        model=model,
        frequency="epoch",
        mode="asynchronous",
        num_workers=4,
    )

    start = time.time()
    spark_model.fit(train_rdd,
                    epochs=args.epochs,
                    batch_size=args.batch,
                    verbose=1,
                    validation_split=0.1)
    train_seconds = time.time() - start

    # Evaluate on the test RDD.
    final_model = spark_model.master_network

    def predict_partition(it):
        rows = list(it)
        if not rows:
            return iter([])
        X = np.array([r[0] for r in rows], dtype=np.float32)
        y = np.array([r[1] for r in rows], dtype=np.float32)
        p = final_model.predict(X, verbose=0).reshape(-1)
        for yi, pi in zip(y, p):
            yield (float(yi), float(pi))

    scored = test_pairs.mapPartitions(predict_partition).cache()

    tp = fp = tn = fn = 0
    for y, p in scored.collect():
        yhat = 1 if p >= 0.5 else 0
        if y == 1 and yhat == 1:
            tp += 1
        elif y == 0 and yhat == 1:
            fp += 1
        elif y == 0 and yhat == 0:
            tn += 1
        else:
            fn += 1

    prec = tp / max(1, tp + fp)
    rec = tp / max(1, tp + fn)
    f1 = 2 * prec * rec / max(1e-9, prec + rec)
    acc = (tp + tn) / max(1, tp + fp + tn + fn)

    pos = scored.filter(lambda x: x[0] == 1).map(lambda x: x[1]).take(20000)
    neg = scored.filter(lambda x: x[0] == 0).map(lambda x: x[1]).take(20000)
    if pos and neg:
        wins = ties = 0
        for p in pos:
            for q in neg:
                if p > q:
                    wins += 1
                elif p == q:
                    ties += 1
        auc = (wins + 0.5 * ties) / (len(pos) * len(neg))
    else:
        auc = float("nan")

    metrics = {
        "model": "elephas_dnn",
        "n_train": n,
        "n_features": len(FEATURE_COLS),
        "epochs": args.epochs,
        "batch": args.batch,
        "train_seconds": train_seconds,
        "auc_roc": auc,
        "precision": prec,
        "recall": rec,
        "f1": f1,
        "accuracy": acc,
        "confusion": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
    }

    # Save the Keras model on the master node, then copy to GCS via Hadoop FS.
    local_path = "/tmp/wikiflow_dnn.h5"
    final_model.save(local_path)

    # Copy local file to GCS using the JVM Hadoop FileSystem API. This works
    # without gcloud and without the gsutil command.
    hconf = sc._jsc.hadoopConfiguration()
    fs = (sc._jvm.org.apache.hadoop.fs.FileSystem
          .get(sc._jvm.java.net.URI.create(args.out), hconf))
    src = sc._jvm.org.apache.hadoop.fs.Path("file://" + local_path)
    dst = sc._jvm.org.apache.hadoop.fs.Path(
        args.out.rstrip("/") + "/wikiflow_dnn.h5")
    fs.copyFromLocalFile(False, True, src, dst)

    sc.parallelize([json.dumps(metrics, indent=2)], 1) \
      .saveAsTextFile(args.metrics + ".tmp")

    print("DNN metrics:", json.dumps(metrics, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
