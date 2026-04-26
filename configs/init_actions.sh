#!/bin/bash
# Dataproc initialization action for WikiFlow.
# Installs Elephas, Keras, kafka-python, plotly, sseclient, and the BigQuery
# connector dependencies on every cluster node.
#
# Upload this file to gs://wikiflow-<id>/configs/init_actions.sh and reference
# it from the Dataproc create-cluster GUI.

set -e

ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role || true)"

# Same set runs on master and workers; conda+pip is the simplest path on the
# Dataproc 2.x images.
PIP="/opt/conda/miniconda3/bin/pip"

$PIP install --quiet --no-cache-dir \
    "tensorflow==2.15.0" \
    "keras==2.15.0" \
    "elephas==4.0.0" \
    "kafka-python==2.0.2" \
    "sseclient-py==1.8.0" \
    "plotly==5.22.0" \
    "google-cloud-bigquery==3.20.1"

# Confirm imports load on every node.
/opt/conda/miniconda3/bin/python - << 'PYEOF'
import elephas, tensorflow, keras, plotly, kafka
print("init_actions ok",
      "elephas", elephas.__version__,
      "tf", tensorflow.__version__,
      "keras", keras.__version__,
      "plotly", plotly.__version__,
      "kafka-python", kafka.__version__)
PYEOF
