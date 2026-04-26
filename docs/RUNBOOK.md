# WikiFlow: Predicting Wikipedia Editor Dropout at Scale

MET CS 777, Big Data Analytics, Boston University, Spring 2026
Aryan Meena and Kunj Manish Kumar Patel

## What this project does

WikiFlow ingests the public English Wikipedia revision history (about 25 GB
compressed, around 60 million events from 2021 through 2025) and predicts
which newly registered editors will have stopped editing six months after
their first ten edits. We compare three models: a from-scratch logistic
regression on Spark RDDs, a from-scratch K-Means clustering on Spark RDDs,
and a Keras deep network trained across Spark workers with Elephas. A live
demo scores the public Wikimedia EventStreams feed in near real time using
Kafka and Spark Structured Streaming. Results land in Google Cloud Storage
and BigQuery and are summarized in a single Plotly HTML dashboard.

## Repo layout

```
WikiFlow/
  README.md                       this file
  BRIEF.md                        partner-facing overview of the whole pipeline
  requirements.txt                Python packages used locally
  configs/
    init_actions.sh               Dataproc init action that installs Elephas, Keras, Kafka client
    dump_urls.txt                 URL list for the GCS Storage Transfer Service
    generate_url_list.py          regenerate the URL list for any year range or wiki
  docs/
    01_gcs_setup.md               click-by-click GCS console steps
    02_dataproc_setup.md          click-by-click Dataproc and Kafka steps
    03_running_jobs.md            argument tables for every Spark job
  src/
    01_clean_and_partition.py     raw TSV.bz2 to Parquet, drops bots and noise
    02_features.py                per-editor feature vectors and dropout label
    03_logreg_rdd.py              custom logistic regression (no MLlib)
    04_kmeans_rdd.py              custom K-Means (no MLlib)
    05_elephas_dnn.py             distributed Keras DNN via Elephas
    06_kafka_streaming.py         Spark Structured Streaming scorer
    07_kafka_producer.py          Wikimedia EventStreams to Kafka producer
    08_eval_and_dashboard.py      metrics, Plotly dashboard, BigQuery export
  report/
    WikiFlow_Report.docx          final written report
  presentation/
    WikiFlow_Slides.pptx          10-minute deck
```

## How to run the project end to end

The project runs entirely from the GCP web console. There is no `gcloud`
command and no terminal step on GCP. The only thing that runs on a laptop
is the optional Kafka producer for the live demo.

1. Read `BRIEF.md`. It explains the pipeline in plain language.
2. Follow `docs/01_gcs_setup.md` to create the bucket, upload the scripts,
   and start the Storage Transfer Service ingest. Wait one to two hours.
3. Follow `docs/02_dataproc_setup.md` to create the Dataproc cluster with
   the init action and provision a Confluent Cloud Kafka cluster.
4. Submit the Spark jobs through the Dataproc Jobs page, in this order:
   - `01_clean_and_partition.py`
   - `02_features.py`
   - `03_logreg_rdd.py`
   - `04_kmeans_rdd.py`
   - `05_elephas_dnn.py`
   - `08_eval_and_dashboard.py`
   The argument table for every job is in `docs/03_running_jobs.md`.
5. For the live streaming demo, run `07_kafka_producer.py` on a laptop and
   submit `06_kafka_streaming.py` to Dataproc.
6. Open `gs://wikiflow-<id>/dashboard/index.html` in any browser, take
   screenshots for the report and the slides.

When you finish a session, stop the Dataproc cluster from the console.
Storage stays cheap, idle compute does not.

## Code style

The code is intentionally short and uses standard PySpark patterns the
course covered. Every file is one job, takes its inputs and outputs as
command-line flags, and writes results to GCS so any other job can pick
them up. There are no hidden config files and no environment variables.

## Results we expect

Targets we set in the proposal:
- Logistic regression AUC-ROC on the held-out test set at or above 0.75.
- Elephas DNN AUC-ROC matching or beating logistic regression.
- K-Means silhouette at or above 0.35 with k=4 archetypes.
- Doubling Dataproc workers from 2 to 4 cuts logistic regression training
  time by at least 35%, confirming distributed speedup.

The actual numbers from the run are written into the report and into
`gs://wikiflow-<id>/dashboard/summary.json`.
