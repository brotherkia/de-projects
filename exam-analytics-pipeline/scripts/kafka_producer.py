"""
Publishes one JSON event to the "exam-attempts" topic per submitted exam
attempt. In the real Django app, call publish_attempt_event(...) from the
attempt-submission view/signal, right after the attempt is scored.

Requires a running Kafka broker (see docker-compose.yml) -- this can't be
exercised in a sandbox with no broker, so event_processing.py carries the
actual tested logic; this file is a thin, carefully-written transport layer
around it.
"""
import json
import os

from kafka import KafkaProducer

TOPIC = "exam-attempts"


def get_producer() -> KafkaProducer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",  # wait for the broker to confirm the write, not just the send
        retries=3,
    )


def publish_attempt_event(producer: KafkaProducer, attempt_id: int, user_id: int, subject_name: str,
                           correct_answers: int, total_answers: int, submitted_at: str) -> None:
    event = {
        "attempt_id": attempt_id,
        "user_id": user_id,
        "subject_name": subject_name,
        "correct_answers": correct_answers,
        "total_answers": total_answers,
        "submitted_at": submitted_at,
    }
    # Keying by subject_name means all events for one subject land on the same
    # partition, so a single consumer instance sees them in submission order.
    producer.send(TOPIC, key=subject_name, value=event)


if __name__ == "__main__":
    # Small demo -- publishes a handful of sample events. Run this against a
    # real broker (docker-compose up kafka) to see them land in the topic.
    producer = get_producer()
    demo_events = [
        (1, 101, "Biology", 8, 10, "2026-01-01T10:00:00"),
        (2, 102, "Chemistry", 4, 10, "2026-01-01T10:05:00"),
        (3, 103, "Biology", 9, 10, "2026-01-01T10:10:00"),
    ]
    for e in demo_events:
        publish_attempt_event(producer, *e)
    producer.flush()
    print(f"Published {len(demo_events)} events to '{TOPIC}'.")
