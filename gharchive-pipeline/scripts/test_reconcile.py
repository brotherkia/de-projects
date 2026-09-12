"""
Prove the streaming path and the batch path produce identical aggregates.

This is the test the whole Kafka layer exists for. Two independent
implementations -- pandas groupby over a whole hour, versus a per-message
running counter -- read byte-identical input and must agree on every repo and
every column. Agreement is evidence both are right; a single differing row
means one of them is wrong and names it.

Assumes the hour is already in BOTH tables:

    python3 gh_producer.py 2026-09-12-06
    python3 gh_consumer.py 2026-09-12-06
    python3 test_reconcile.py 2026-09-12-06

Exits non-zero on any mismatch, so it works as a CI gate.
"""
import argparse
import sys

import pandas as pd

from gharchive import REPO_ACTIVITY_TABLE, get_connection
from gh_consumer import STREAMING_TABLE

METRICS = ["events", "distinct_actors", "stars_gained", "forks", "prs_opened", "prs_merged"]

results = []


def check(name, passed, detail=""):
    results.append((name, passed))
    print(f"  {'PASS' if passed else 'FAIL'}  {name}{'  -- ' + detail if detail else ''}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("hour", help="e.g. 2026-09-12-06")
    args = ap.parse_args()
    hour = args.hour

    conn = get_connection()
    try:
        batch = pd.read_sql_query(
            f"SELECT * FROM {REPO_ACTIVITY_TABLE} WHERE event_hour = '{hour}'", conn)
        stream = pd.read_sql_query(
            f"SELECT * FROM {STREAMING_TABLE} WHERE event_hour = '{hour}'", conn)
    finally:
        conn.close()

    print(f"\nreconciling {hour}: batch={len(batch):,} rows, streaming={len(stream):,} rows")

    if batch.empty or stream.empty:
        print("FAIL  one side is empty -- run gh_producer.py and gh_consumer.py first")
        return 1

    print("\n[1] the same repos appear on both sides")
    b_repos, s_repos = set(batch.repo_name), set(stream.repo_name)
    check("row counts match", len(batch) == len(stream), f"{len(batch)} vs {len(stream)}")
    check("no repo only in batch", not (b_repos - s_repos),
          f"{len(b_repos - s_repos)} missing from stream: {sorted(b_repos - s_repos)[:3]}")
    check("no repo only in streaming", not (s_repos - b_repos),
          f"{len(s_repos - b_repos)} extra in stream: {sorted(s_repos - b_repos)[:3]}")

    print("\n[2] every metric agrees, repo by repo")
    merged = batch.merge(stream, on=["event_hour", "repo_name"], suffixes=("_b", "_s"))
    for m in METRICS:
        diff = merged[merged[f"{m}_b"] != merged[f"{m}_s"]]
        if diff.empty:
            check(f"{m} identical across {len(merged):,} repos", True)
        else:
            worst = diff.head(3)[["repo_name", f"{m}_b", f"{m}_s"]].to_dict("records")
            check(f"{m} identical across {len(merged):,} repos", False,
                  f"{len(diff)} repos differ, e.g. {worst}")

    print("\n[3] totals agree")
    for m in METRICS:
        tb, ts = int(batch[m].sum()), int(stream[m].sum())
        check(f"total {m}", tb == ts, f"{tb:,} vs {ts:,}")

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("FAILED:", ", ".join(failed))
        return 1
    print(f"Streaming and batch agree exactly for {hour}: "
          f"{len(merged):,} repos, {int(batch['events'].sum()):,} events.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
