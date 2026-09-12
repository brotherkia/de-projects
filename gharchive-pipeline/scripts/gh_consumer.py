"""
Consume one hour of replayed GH Archive events and aggregate them incrementally.

The whole point of this file is that it computes the SAME numbers as
scripts/gharchive.py by a deliberately DIFFERENT method:

    batch      pandas groupby over a DataFrame holding the whole hour at once
    streaming  a running counter per repo, updated one message at a time,
               never holding the hour in a frame

If two independent implementations over identical input agree exactly, both are
almost certainly right. If they disagree by one row, one of them is wrong and
the reconciliation says so. That is a far stronger claim than "the streaming
job ran without errors".

Writes gh_repo_activity_streaming, whose shape matches gh_repo_activity_hourly
column for column so the comparison is a plain SQL join.

    python3 gh_consumer.py 2026-09-12-06
"""
import argparse
import json
import os
import sys
from collections import defaultdict

import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition

from gharchive import REPO_ACTIVITY_TABLE, get_connection, load_partition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("GH_TOPIC", "gh.events.v1")
STREAMING_TABLE = "gh_repo_activity_streaming"

COLS = ["event_hour", "repo_name", "events", "distinct_actors",
        "stars_gained", "forks", "prs_opened", "prs_merged"]


def ensure_streaming_schema(conn) -> None:
    """Same columns and same primary key as the batch table, so they can be joined directly."""
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {STREAMING_TABLE} (
            event_hour      TEXT    NOT NULL,
            repo_name       TEXT    NOT NULL,
            events          INTEGER NOT NULL,
            distinct_actors INTEGER NOT NULL,
            stars_gained    INTEGER NOT NULL,
            forks           INTEGER NOT NULL,
            prs_opened      INTEGER NOT NULL,
            prs_merged      INTEGER NOT NULL,
            PRIMARY KEY (event_hour, repo_name)
        )
    """)
    conn.commit()


class RepoState:
    """
    Running totals for one repo. The streaming analogue of one groupby row.

    distinct_actors needs a set rather than a counter, and that is the honest
    cost of doing this incrementally: cardinality is the one aggregate you
    cannot keep in O(1) exactly. For one hour (~25-45k repos) the sets are
    small and exactness is worth the memory. At a scale where it is not, this
    is exactly where HyperLogLog gets introduced -- and where the numbers stop
    matching the batch path exactly, which is a trade to make deliberately
    rather than discover.
    """
    __slots__ = ("events", "actors", "stars", "forks", "prs_opened", "prs_merged")

    def __init__(self):
        self.events = 0
        self.actors = set()
        self.stars = 0
        self.forks = 0
        self.prs_opened = 0
        self.prs_merged = 0

    def update(self, rec: dict) -> None:
        self.events += 1
        self.actors.add(rec["actor_login"])
        etype = rec["event_type"]
        if etype == "WatchEvent":       # WatchEvent is a star, not a watch
            self.stars += 1
        elif etype == "ForkEvent":
            self.forks += 1
        elif etype == "PullRequestEvent":
            if rec.get("pr_action") == "opened":
                self.prs_opened += 1
            if rec.get("pr_merged"):
                self.prs_merged += 1


def consume_hour(event_hour: str, group: str, idle_timeout: float = 15.0) -> tuple:
    """
    Read the topic from the beginning and fold every message for `event_hour`.

    Reads from the start of every partition with a throwaway group id, so the
    run is deterministic and repeatable -- a reconciliation that depends on
    where a previous run left its offsets proves nothing.

    enable.partition.eof gives an explicit end-of-partition signal, so the loop
    stops when it has genuinely drained every partition rather than guessing
    from a silence timeout.
    """
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })

    md = consumer.list_topics(TOPIC, timeout=20)
    if TOPIC not in md.topics or md.topics[TOPIC].error is not None:
        raise SystemExit(f"topic {TOPIC} not found -- run gh_producer.py first")
    parts = list(md.topics[TOPIC].partitions.keys())
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in parts])  # offset 0 = from the start

    state = defaultdict(RepoState)
    consumed = other_hour = 0
    eof = set()
    try:
        while len(eof) < len(parts):
            msg = consumer.poll(idle_timeout)
            if msg is None:
                raise SystemExit(
                    f"no message for {idle_timeout}s with {len(eof)}/{len(parts)} partitions "
                    f"drained -- broker stalled or topic incomplete")
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(msg.partition())
                    continue
                raise SystemExit(f"consumer error: {msg.error()}")
            rec = json.loads(msg.value())
            consumed += 1
            # The topic outlives any one replay, so a run must only fold the
            # hour it was asked for.
            if rec["event_hour"] != event_hour:
                other_hour += 1
                continue
            state[rec["repo_name"]].update(rec)
    finally:
        consumer.close()

    rows = [
        {
            "event_hour": event_hour,
            "repo_name": repo,
            "events": st.events,
            "distinct_actors": len(st.actors),
            "stars_gained": st.stars,
            "forks": st.forks,
            "prs_opened": st.prs_opened,
            "prs_merged": st.prs_merged,
        }
        for repo, st in state.items()
    ]
    df = pd.DataFrame(rows, columns=COLS) if rows else pd.DataFrame(columns=COLS)
    return df, consumed, other_hour


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("hour", help="e.g. 2026-09-12-06 (padded storage key)")
    ap.add_argument("--group", default=None, help="consumer group; default is a fresh one per run")
    args = ap.parse_args()

    event_hour = args.hour
    group = args.group or f"gh-reconcile-{os.getpid()}"

    df, consumed, other_hour = consume_hour(event_hour, group)
    print(f"{event_hour}: consumed {consumed:,} messages "
          f"({other_hour:,} belonged to other hours), {len(df):,} repos aggregated")

    conn = get_connection()
    try:
        ensure_streaming_schema(conn)
        # Same delete-then-insert the batch path uses. Kafka is at-least-once:
        # a rebalance or a restart redelivers messages. An idempotent load by
        # partition is what turns at-least-once delivery into effectively-once
        # RESULTS, which is the only kind of exactly-once that matters here.
        n = load_partition(conn, df, STREAMING_TABLE, event_hour)
    finally:
        conn.close()
    print(f"loaded {n:,} rows into {STREAMING_TABLE} for {event_hour}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
