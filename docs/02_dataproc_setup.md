# Dataproc + Kafka Setup (GUI Only)

## A. Create the Dataproc cluster

1. Go to https://console.cloud.google.com.
2. Top search bar: "Dataproc". Open the Dataproc page.
3. Click Create Cluster. Pick "Cluster on Compute Engine". Click Create.

Fill in this form:

- Cluster name: `wikiflow-cluster`
- Region: `us-central1`
- Zone: any
- Cluster type: Standard (1 master, N workers)
- Image version: 2.2-debian12 (any 2.x is fine, the init action handles versions)
- Optional components: leave empty for now (we install Elephas via init action)

Section "Configure nodes":

- Manager node: 1 x n2-standard-4, 100 GB pd-balanced
- Worker nodes: 4 x n2-standard-4, 100 GB pd-balanced

Section "Customize cluster":

- Initialization actions: Browse -> select `gs://wikiflow-<id>/configs/init_actions.sh`. Set Executable execution timeout to 600s.
- Internal IP only: leave off (we want internet for the init action).
- Cloud Storage staging bucket: pick `wikiflow-<id>`.

Section "Manage security": leave defaults.

Click Create. Cluster goes to Running in about five minutes.

## B. Verify the cluster

1. Open the new cluster in the Dataproc page.
2. Tab "VM Instances": all four workers and master should be green.
3. Tab "Web Interfaces": click YARN ResourceManager. You should see four NodeManagers active.
4. Open any worker VM in Compute Engine and check the init action log under `/var/log/dataproc-initialization-script-0.log` to confirm Elephas and kafka-python installed cleanly. Skip this if everything else looks fine.

## C. Provision Kafka (only needed for the streaming demo)

The simplest GUI-only Kafka is Confluent Cloud (free tier):

1. Sign up at https://confluent.cloud (free, $400 credit).
2. Create environment "wikiflow", cluster "wiki-edits-cluster" in region matching us-central1 (Confluent calls it "us-central1 GCP").
3. Cluster type: Basic.
4. Click into the cluster -> Topics -> Create topic.
   - Name: `wiki-edits`
   - Partitions: 3
   - Retention: 1 day
5. Cluster -> API Keys -> Create key. Save the key and secret somewhere safe.
6. Cluster -> Cluster Settings -> copy the Bootstrap Server.

We will paste these three values (bootstrap server, key, secret) into the Spark job arguments later.

If you would rather host Kafka yourself, create one Compute Engine VM with the GCP Marketplace "Kafka by Bitnami" image and use its external IP. The Confluent route is faster.

## D. Stop the cluster when not in use

This is the single most important step for cost.

1. Dataproc -> Clusters -> tick `wikiflow-cluster`.
2. Click STOP at the top.
3. To resume work, tick the cluster and click START.

Stopped clusters cost only the disk (cents per day). Running clusters with 5 VMs cost roughly $0.80 per hour.

When you finish the project, click DELETE.

You are done with cluster setup. Move on to `03_running_jobs.md`.
