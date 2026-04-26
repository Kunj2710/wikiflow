"""
07_kafka_producer.py
Read the public Wikimedia EventStreams SSE feed of every Wikipedia edit and
republish each edit, in a small JSON envelope, to a Kafka topic that our
Spark Structured Streaming job consumes.

This script runs locally on a laptop. It does not need GCP.

Run:
    pip install kafka-python sseclient-py requests
    python 07_kafka_producer.py \
        --bootstrap pkc-XXX.us-central1.gcp.confluent.cloud:9092 \
        --api-key  YOUR_KEY \
        --api-secret YOUR_SECRET \
        --topic wiki-edits

Filter to English Wikipedia revision events only.
"""

import argparse
import json
import sys
import time

import requests
import sseclient
from kafka import KafkaProducer


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--topic", default="wiki-edits")
    parser.add_argument("--wiki", default="enwiki")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=args.api_key,
        sasl_plain_password=args.api_secret,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=200,
    )

    print(f"Connecting to {STREAM_URL}", flush=True)
    response = requests.get(STREAM_URL, stream=True, timeout=60)
    client = sseclient.SSEClient(response)

    sent = 0
    last_log = time.time()
    for ev in client.events():
        if ev.event != "message":
            continue
        try:
            data = json.loads(ev.data)
        except Exception:
            continue
        if data.get("wiki") != args.wiki:
            continue
        if data.get("type") != "edit":
            continue
        # Drop bots; the dump cleaning step does the same.
        if data.get("bot"):
            continue

        envelope = {
            "user": data.get("user"),
            "title": data.get("title"),
            "namespace": int(data.get("namespace") or 0),
            "ts": int(data.get("timestamp") or 0),
            "bytes_diff": int(data.get("length", {}).get("new", 0)) -
                          int(data.get("length", {}).get("old", 0)),
            "comment": data.get("comment", ""),
            "minor": 1 if data.get("minor") else 0,
            "is_revert": 1 if "mw-revert" in (data.get("tags") or []) else 0,
        }
        producer.send(args.topic, envelope)
        sent += 1
        if time.time() - last_log > 5:
            print(f"sent={sent}", flush=True)
            last_log = time.time()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nstopped")
        sys.exit(0)
