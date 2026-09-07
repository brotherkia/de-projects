"""
Consumes from "exam-attempts" and keeps live_subject_performance up to date
in near-real-time, using the same process_attempt_event() logic that's
already validated against the batch pipeline (see test_event_processing.py).

This is the "speed layer" alongside the Airflow DAG's nightly "batch layer" --
same underlying question (subject pass rates), two different latencies.
"""
import json
import os

from kafka import KafkaConsumer

from event_processing import new_running_stats, process_attempt_event, running_stats_to_rows
from pipeline_functions import get_connection, load_dataframe

TOPIC = "exam-attempts"


def get_consumer() -> KafkaConsumer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="exam-analytics-live",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def run(flush_every: int = 20):
    """
    Consumes indefinitely, updating running stats in memory and periodically
    (every `flush_every` events) writing them to live_subject_performance so
    a dashboard/API can read current numbers without hitting Kafka directly.
    """
    import pandas as pd

    consumer = get_consumer()
    stats = new_running_stats()
    conn = get_connection()
    count = 0

    print(f"Listening on '{TOPIC}'...")
    for message in consumer:
        process_attempt_event(message.value, stats)
        count += 1

        if count % flush_every == 0:
            rows = running_stats_to_rows(stats)
            load_dataframe(conn, pd.DataFrame(rows), "live_subject_performance")
            print(f"[{count} events] refreshed live_subject_performance ({len(rows)} subjects)")


if __name__ == "__main__":
    run()
