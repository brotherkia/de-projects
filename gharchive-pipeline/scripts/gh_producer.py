"""
Replay one hour of GH Archive events into a Kafka topic.

This is the "stream" side of a batch-vs-streaming reconciliation. It reads the
exact same raw file the Airflow DAG reads, and publishes one message per usable
event, so the two paths start from byte-identical input. Anything they disagree
about afterwards is a bug in one of them, not a difference in what they saw.

Zero Airflow imports, like everything else in scripts/. Run it directly:

    python3 gh_producer.py 2026-09-12-06
    python3 gh_producer.py 2026-09-12-06 --limit 5000     # quick smoke test
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer

from gharchive import hour_partition, is_merged_pr, is_usable, iter_events, download_hour

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("GH_TOPIC", "gh.events.v1")


def parse_hour(s: str) -> datetime:
    y, m, d, h = (int(x) for x in s.split("-"))
    return datetime(y, m, d, h, tzinfo=timezone.utc)


def build_message(event: dict, event_hour: str) -> tuple:
    """
    Narrow an event down to the fields the aggregate needs, and pick its key.

    The KEY IS THE DESIGN DECISION here. Kafka guarantees ordering only within
    a partition, and routes by hash(key). Keying on repo_name puts every event
    for a repo on one partition, so a consumer can accumulate that repo's
    counters with no cross-partition coordination and no locking -- the repo is
    the unit of aggregation, so the repo is the key.

    Key on event id instead and the same repo's events scatter across
    partitions; the counts still come out right for a single consumer, but the
    moment you scale to one consumer per partition each holds a fragment of
    every repo and you need a shuffle to finish the job. That is the difference
    between a design that scales out and one that only happens to work.
    """
    payload = event.get("payload") or {}
    pr = payload.get("pull_request") or {}
    etype = event["type"]
    record = {
        "event_hour": event_hour,
        "event_type": etype,
        "repo_name": event["repo"]["name"],
        "actor_login": event["actor"]["login"],
        "created_at": event["created_at"],
        "pr_action": payload.get("action") if etype == "PullRequestEvent" else None,
        "pr_merged": is_merged_pr(etype, payload, pr),
    }
    return record["repo_name"].encode(), json.dumps(record).encode()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("hour", help="e.g. 2026-09-12-06")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    args = ap.parse_args()

    dt = parse_hour(args.hour)
    event_hour = hour_partition(dt)
    path = download_hour(dt, args.data_dir)

    # linger.ms batches small messages instead of paying a round trip each;
    # without it this takes minutes rather than seconds for ~100k events.
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "batch.size": 1 << 20,
        "compression.type": "lz4",
        "acks": "all",
    })

    sent = skipped = 0
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))

    for event, _ in iter_events(path):
        if not is_usable(event):
            skipped += 1
            continue
        key, value = build_message(event, event_hour)
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                # Local queue full: let delivery callbacks drain it. Blocking
                # here is correct -- dropping the event silently is how a
                # streaming path quietly stops matching its batch twin.
                producer.poll(0.5)
        sent += 1
        if args.limit and sent >= args.limit:
            break
        if sent % 20000 == 0:
            producer.poll(0)
            print(f"  ...{sent:,} produced")

    producer.flush(120)
    print(f"{event_hour}: produced {sent:,} events to {TOPIC} "
          f"({skipped} unusable skipped, {len(failures)} delivery failures)")
    if failures:
        print("FAILURES:", failures[:5], file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
