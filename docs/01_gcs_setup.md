# GCS Console Setup (GUI Only)

Everything in this file happens in the browser. No `gcloud`, no terminal.

## A. Create the project

1. Go to https://console.cloud.google.com.
2. Top bar -> project dropdown -> New Project.
3. Name it `wikiflow`. Click Create.
4. Wait until the dropdown switches to the new project.

## B. Enable the APIs we need

Go to the search bar at the top, search and click Enable for each one:

- Cloud Storage API
- Dataproc API
- Storage Transfer API
- BigQuery API
- Compute Engine API

If a button says Enable, click it. If it says Manage, you are done.

## C. Create the bucket

1. Left menu -> Cloud Storage -> Buckets -> Create.
2. Name: `wikiflow-<your initials>` (must be globally unique, try `wikiflow-am-2026` for Aryan).
3. Location type: Region. Region: `us-central1` (cheapest, same region as our Dataproc cluster).
4. Storage class: Standard.
5. Access control: Uniform.
6. Public access prevention: Enforced.
7. Soft delete: leave default.
8. Click Create.

## D. Create the folder layout inside the bucket

Open the new bucket. Click Create folder for each of these (one at a time):

```
raw/
clean/
features/
models/
metrics/
dashboard/
scripts/
configs/
checkpoints/
```

## E. Upload the helper files into the bucket

In the bucket browser:

1. Open `scripts/`. Click Upload files. Pick all eight `.py` files from `WikiFlow/src/`.
2. Open `configs/`. Click Upload files. Pick `init_actions.sh` and `dump_urls.txt` from `WikiFlow/configs/`.

You will refer to these by `gs://wikiflow-<id>/scripts/01_clean_and_partition.py` etc. when submitting Dataproc jobs.

## F. Pull the Wikipedia dumps with Storage Transfer Service

This is the part that ingests roughly 25 GB without using your laptop bandwidth.

1. Left menu -> Storage Transfer Service -> Create transfer job.
2. Source type: URL list.
3. Click Choose URL list and upload `configs/dump_urls.txt` (or paste in the URLs directly if the upload field allows it).
   - The file is a plain text TSV with one URL per line plus the file size and an md5 if available. Storage Transfer parses it natively.
4. Destination type: Google Cloud Storage. Bucket: `wikiflow-<id>`. Path: `raw/`.
5. Job name: `wikiflow-ingest`.
6. Schedule: Run once, starting now.
7. Settings: keep defaults. Overwrite when source is newer = on. Delete from source = off.
8. Click Create. Wait one to two hours. The console shows a per-file progress bar.

When it finishes you should see roughly 60 files in `raw/`, each between 200 MB and 600 MB, named like `enwiki.2023-04.tsv.bz2`.

## G. Sanity check from the browser

1. Open `raw/`. Click any one TSV.bz2 file. Check that the size matches what Storage Transfer reported.
2. Check Object Versioning is off (under bucket Configuration). We do not want extra storage cost.

You are done with GCS setup. Move on to `02_dataproc_setup.md`.
