# WikiFlow Project Brief

This brief is for our partner. Read it once and you will know exactly what we are building, how the pipeline runs end to end, and what each file in this folder does.

## 1. The Question We Are Answering

When a brand new editor signs up on Wikipedia and makes their first edits, can we predict whether they will still be editing six months later? Wikipedia has lost a big chunk of its active editor base since 2007 and the Wikimedia Foundation pays researchers to study this exact problem. We are doing it at scale on the full English Wikipedia revision history.

## 2. The Dataset

Source: https://dumps.wikimedia.org/other/mediawiki_history/

Files we use: `enwiki` monthly TSV.bz2 dumps from January 2021 through December 2025. About 60 million revision events, around 25 GB compressed. Schema is already documented by the Wikimedia Analytics team and includes ready-made columns like `event_user_revision_count`, `revision_is_identity_revert`, and `event_user_is_bot_by`. No JSON parsing, no API throttling.

## 3. The Stack

| Layer | Tool | Why |
|---|---|---|
| Object storage | Google Cloud Storage (GCS) | Holds raw dumps, Parquet, models, results, dashboard |
| Ingestion | GCS Storage Transfer Service | Pure GUI pull from dumps.wikimedia.org into the bucket |
| Cluster | Google Cloud Dataproc (1 master + 4 workers) | Managed Spark, talks to GCS natively |
| Batch processing | Apache Spark (PySpark) | Distributed feature engineering on tens of millions of rows |
| Custom ML | PySpark RDDs | From-scratch logistic regression and K-Means, no MLlib |
| Distributed deep learning | Elephas + Keras | Trains a Keras DNN across Spark workers |
| Streaming source | Wikimedia EventStreams (SSE) | Live feed of every edit as it happens |
| Streaming bus | Apache Kafka | Buffers live edits between producer and Spark |
| Streaming compute | Spark Structured Streaming | Reads Kafka, scores edits in near real time |
| Serving / SQL layer | BigQuery | Final tables that anyone can query without Spark |
| Visualization | Plotly HTML dashboard saved on GCS | One link, opens in any browser |

## 4. End-to-End Pipeline

```
dumps.wikimedia.org
        |
        |  Storage Transfer Service (GUI, one-time)
        v
gs://wikiflow-<id>/raw/                <-- TSV.bz2 files
        |
        |  01_clean_and_partition.py  (Spark)
        v
gs://wikiflow-<id>/clean/              <-- Parquet, partitioned by year/month
        |
        |  02_features.py             (Spark)
        v
gs://wikiflow-<id>/features/           <-- one row per editor, label + 12 features
        |
        |  03_logreg_rdd.py           (custom RDD logreg)
        |  04_kmeans_rdd.py           (custom RDD K-Means)
        |  05_elephas_dnn.py          (Elephas distributed DNN)
        v
gs://wikiflow-<id>/models/             <-- weights, centroids, keras model
gs://wikiflow-<id>/metrics/            <-- json: AUC, F1, silhouette, training time
        |
        |  08_eval_and_dashboard.py
        v
gs://wikiflow-<id>/dashboard/index.html
BigQuery dataset wikiflow.results

Live side (parallel, optional):
EventStreams  -->  07_kafka_producer.py  -->  Kafka topic 'wiki-edits'
                                             |
                                             v
                                  06_kafka_streaming.py
                                  (Spark Structured Streaming
                                   loads logreg weights, scores
                                   each event, writes to GCS sink)
```

## 5. The Features We Compute Per Editor

For every editor, we look only at their first 10 edits (the observation window) and build:

1. `velocity` average edits per day during the window
2. `revert_rate` fraction of those edits the community reverted
3. `namespace_diversity` Shannon entropy across page namespaces touched
4. `avg_bytes_added` mean of `revision_text_bytes_diff` clipped at zero
5. `avg_bytes_removed` mean of negative `revision_text_bytes_diff`
6. `talk_page_ratio` fraction of edits in namespaces 1, 3, 5 (Talk pages)
7. `peak_hour_sin`, `peak_hour_cos` cyclical encoding of the editor's modal edit hour
8. `weekend_ratio` fraction of edits on Saturday/Sunday
9. `edit_summary_rate` fraction of edits with non-empty `revision_comment`
10. `minor_edit_ratio` fraction of edits flagged as minor
11. `session_count` number of distinct edit sessions (gap > 30 min)
12. `first_edit_size` size in bytes of the very first edit

Label: `dropout = 1` if the editor made zero edits in the six months after the observation window, else `0`.

## 6. The Models

**Custom logistic regression** (`03_logreg_rdd.py`). RDD of `(label, feature vector)`, mini-batch SGD, weight updates aggregated with `treeAggregate`. Reports AUC, precision, recall, F1, confusion matrix.

**Custom K-Means** (`04_kmeans_rdd.py`). RDD-based, k=4, broadcast centroids, recompute by `reduceByKey`. Reports silhouette and the mean feature vector per cluster so we can name the archetypes (Power editor, Casual, Frustrated newcomer, Bot-like).

**Elephas DNN** (`05_elephas_dnn.py`). Keras Sequential model (12 then 64 then 32 then 1, ReLU and sigmoid), wrapped in `SparkModel`, trained asynchronously across workers. Reports the same metrics as logreg so we can compare.

**Streaming scorer** (`06_kafka_streaming.py`). Loads logreg weights from GCS, reads the `wiki-edits` Kafka topic, computes a partial feature vector for the editor of each new edit, writes a streaming risk score to GCS every minute.

## 7. How We Run It (One Time)

1. Open the GCS console and create a bucket named `wikiflow-<your-initials>`.
2. Open Storage Transfer Service in the console, paste the URL list from `configs/dump_urls.txt` (we generated it for you), let it run. About one to two hours.
3. Open Dataproc, create a cluster called `wikiflow-cluster` with the init action `configs/init_actions.sh` from the bucket. About five minutes.
4. From the Dataproc Jobs page in the console, submit each script in order: `01_clean_and_partition.py`, `02_features.py`, then the three model scripts in any order, then `08_eval_and_dashboard.py`.
5. For the live demo, start a small Confluent Cloud Kafka cluster, run `07_kafka_producer.py` on any laptop with internet, and submit `06_kafka_streaming.py` to Dataproc.
6. Open `gs://wikiflow-<id>/dashboard/index.html` in a browser, take screenshots for the report.

Detailed click-by-click steps are in `docs/01_gcs_setup.md` and `docs/02_dataproc_setup.md`.

## 8. What Each File Does

```
WikiFlow/
  README.md                       run instructions for the grader
  BRIEF.md                        this file, the partner overview
  requirements.txt                pinned Python deps for local dev
  configs/
    init_actions.sh               Dataproc init action, installs Elephas + Kafka client
    dump_urls.txt                 list of MediaWiki dump URLs, paste into Storage Transfer
    generate_url_list.py          regenerate dump_urls.txt for any year range
  docs/
    01_gcs_setup.md               GUI steps for bucket and Storage Transfer
    02_dataproc_setup.md          GUI steps for Dataproc + Kafka
    03_running_jobs.md            GUI steps for submitting each job
  src/
    01_clean_and_partition.py     raw TSV -> partitioned Parquet
    02_features.py                Parquet -> per-editor feature table
    03_logreg_rdd.py              custom logistic regression (no MLlib)
    04_kmeans_rdd.py              custom K-Means (no MLlib)
    05_elephas_dnn.py             distributed Keras DNN via Elephas
    06_kafka_streaming.py         Spark Structured Streaming scorer
    07_kafka_producer.py          Wikimedia EventStreams to Kafka
    08_eval_and_dashboard.py      metrics, Plotly dashboard, BigQuery export
  report/
    WikiFlow_Report.docx          final written report
  presentation/
    WikiFlow_Slides.pptx          10-minute deck
```

## 9. Who Does What

You can split the work like this:

- Aryan: cluster and bucket setup, ingestion, scripts 01, 02, 03, 08, report.
- Kunj: scripts 04, 05, 06, 07, presentation.

Either of us can do either side. Pick what you want and just open a PR on the GitHub repo we will create.

## 10. What Success Looks Like

- AUC-ROC on held-out editors at or above 0.75 with custom logreg.
- DNN should match or beat that.
- K-Means silhouette at or above 0.35.
- Doubling Dataproc workers from 2 to 4 should cut training time by 35% or more.
- Live Kafka demo prints a risk score for each new edit within a few seconds.
- Dashboard saved to GCS opens cleanly in any browser.

That is the whole project on one page. Anything you change, write it back into this file so we both stay in sync.
