# Running the Jobs (GUI Only)

All jobs are submitted from the Dataproc Jobs page. No terminal needed.

## How to submit any PySpark job

1. Dataproc -> Jobs -> Submit Job.
2. Cluster: `wikiflow-cluster`.
3. Job type: PySpark.
4. Main python file: `gs://wikiflow-<id>/scripts/<file>.py`.
5. Arguments: see per-job table below.
6. Properties: leave default unless noted.
7. Jar files: see per-job table below.
8. Click Submit.

The job appears in the Jobs list. Click it to watch logs in real time. Spark History link appears once it finishes.

## Per-job arguments

Replace `<id>` everywhere with your bucket suffix.

### 01_clean_and_partition.py

- Args:
  - `--raw gs://wikiflow-<id>/raw/`
  - `--out gs://wikiflow-<id>/clean/`
- Jars: none.
- Run time on 4 workers: ~25 min.

### 02_features.py

- Args:
  - `--clean gs://wikiflow-<id>/clean/`
  - `--out gs://wikiflow-<id>/features/`
  - `--window 10`
  - `--horizon-days 180`
- Jars: none.
- Run time: ~15 min.

### 03_logreg_rdd.py

- Args:
  - `--features gs://wikiflow-<id>/features/`
  - `--out gs://wikiflow-<id>/models/logreg/`
  - `--metrics gs://wikiflow-<id>/metrics/logreg.json`
  - `--epochs 30`
  - `--lr 0.1`
  - `--batch 4096`
- Jars: none.
- Run time: ~10 min.

### 04_kmeans_rdd.py

- Args:
  - `--features gs://wikiflow-<id>/features/`
  - `--out gs://wikiflow-<id>/models/kmeans/`
  - `--metrics gs://wikiflow-<id>/metrics/kmeans.json`
  - `--k 4`
  - `--iters 30`
- Jars: none.
- Run time: ~5 min.

### 05_elephas_dnn.py

- Args:
  - `--features gs://wikiflow-<id>/features/`
  - `--out gs://wikiflow-<id>/models/dnn/`
  - `--metrics gs://wikiflow-<id>/metrics/dnn.json`
  - `--epochs 15`
  - `--batch 256`
- Properties (paste into Properties field as key=value, one per line):
  - `spark.executorEnv.PYSPARK_PYTHON=/opt/conda/miniconda3/bin/python`
- Jars: none (Elephas installed by init action).
- Run time: ~20 min.

### 08_eval_and_dashboard.py

- Args:
  - `--features gs://wikiflow-<id>/features/`
  - `--metrics gs://wikiflow-<id>/metrics/`
  - `--models gs://wikiflow-<id>/models/`
  - `--dashboard gs://wikiflow-<id>/dashboard/`
  - `--bq-dataset wikiflow`
  - `--bq-table results`
- Jars: `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar`
- Run time: ~3 min.

### 06_kafka_streaming.py (live demo)

- Args:
  - `--bootstrap <confluent-bootstrap-server>:9092`
  - `--api-key <key>`
  - `--api-secret <secret>`
  - `--topic wiki-edits`
  - `--weights gs://wikiflow-<id>/models/logreg/weights.json`
  - `--checkpoint gs://wikiflow-<id>/checkpoints/streaming/`
  - `--out gs://wikiflow-<id>/streaming_predictions/`
- Jars: `gs://wikiflow-<id>/configs/spark-sql-kafka-0-10_2.12-3.5.0.jar` (download from Maven Central, drop into bucket once)
- Run time: streams forever. Stop the job manually when you have enough screenshots.

### 07_kafka_producer.py (run from your laptop while 06 is running)

This one runs locally, not on Dataproc:

```
pip install kafka-python sseclient-py requests
python 07_kafka_producer.py --bootstrap <bs>:9092 --api-key <k> --api-secret <s> --topic wiki-edits
```

You will see one line per Wikipedia edit it forwards to Kafka.

## Suggested run order (first time)

01 -> 02 -> 03 -> 04 -> 05 -> 08, then start 07 locally and submit 06 for the live demo.

## After every session: stop the cluster

Dataproc -> Clusters -> tick cluster -> STOP.
