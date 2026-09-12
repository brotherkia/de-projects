"""
Local tests for the GH Archive pipeline -- no Airflow, no containers.

These check the five properties that decide whether this pipeline survives
contact with a real scheduler:

  1. Dirty data    -- one real malformed event must not kill an hour.
  2. URL naming    -- the archive's hour is not zero-padded; getting that wrong
                      produces a believable URL that 404s.
  3. Conservation  -- the aggregates must add up to the events that went in.
                      A transform that quietly drops rows is worse than one
                      that crashes, because it still produces a plausible number.
  4. Idempotency   -- re-running an hour must not change the result.
                      Airflow retries tasks and backfills overlap scheduled
                      runs; without this, numbers silently double.
  5. Missing hours -- a 404 must surface as HourNotAvailable, not a crash,
                      so the DAG can skip the partition instead of wedging.

Run:  python3 test_gharchive.py
Uses an hour already downloaded into scripts/data/ if present, so the normal
case costs no network.
"""
import os
import sqlite3
import sys
from datetime import datetime, timezone

import pandas as pd

from gharchive import (
    EVENT_TYPE_TABLE,
    REPO_ACTIVITY_TABLE,
    HourNotAvailable,
    IncompleteDownload,
    download_hour,
    completeness_warnings,
    ensure_schema,
    hour_key,
    hour_partition,
    is_usable,
    load_partition,
    read_hour_frame,
    transform_event_types,
    transform_repo_activity,
)

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
TEST_HOUR = datetime(2026, 9, 5, 10, tzinfo=timezone.utc)
TEST_HOUR_KEY = "2026-09-05-10"

# A genuinely missing hour, confirmed 404 against the live archive: the hour
# of the 2016 Dyn DDoS attack. Verified by scanning all 24 hours of that day --
# 23 return 200, hour 18 returns 404.
MISSING_HOUR = datetime(2016, 10, 21, 18, tzinfo=timezone.utc)

results = []


def check(name, passed, detail=""):
    results.append((name, passed))
    print(f"  {'PASS' if passed else 'FAIL'}  {name}{'  -- ' + detail if detail else ''}")


def test_dirty_data():
    """The empty-repo event that crashed the first version of this parser."""
    print("\n[1] dirty data is rejected, not fatal")
    real_bad = {  # shape taken verbatim from 2026-09-05-10, line 2237
        "type": "ForkEvent", "created_at": "2026-09-05T10:02:01Z",
        "actor": {"login": "asd79663"}, "repo": {}, "public": False,
        "payload": {"action": None, "forkee": {}},
    }
    good = {
        "type": "WatchEvent", "created_at": "2026-09-05T10:00:00Z",
        "actor": {"login": "someone"}, "repo": {"name": "org/repo"}, "payload": {},
    }
    check("empty repo{} rejected", is_usable(real_bad) is False)
    check("well-formed event accepted", is_usable(good) is True)
    check("missing actor rejected",
          is_usable({**good, "actor": {}}) is False)
    check("missing type rejected",
          is_usable({**good, "type": None}) is False)


def test_hour_key_format():
    """
    The archive does not zero-pad the hour. Getting this wrong produces a
    plausible-looking URL that 404s, which is exactly how an earlier version
    of this work misread its own bug as a gap in the archive.
    """
    print("\n[2] hour keys match the archive's naming")
    check("hour 3 is '-3', not '-03'",
          hour_key(datetime(2016, 10, 21, 3, tzinfo=timezone.utc)) == "2016-10-21-3")
    check("hour 10 keeps both digits",
          hour_key(datetime(2026, 9, 5, 10, tzinfo=timezone.utc)) == "2026-09-05-10")
    check("month and day ARE zero-padded",
          hour_key(datetime(2026, 1, 2, 0, tzinfo=timezone.utc)) == "2026-01-02-0")


def test_hour_partition_sorts():
    """
    The storage key must order chronologically, which the archive's URL key
    does not. This is the bug migration 001 fixes.

    Caught only after 40 hours were already loaded, because equality was never
    affected -- the loads match with WHERE event_hour = ?, so idempotency held
    the whole time while ORDER BY quietly lied.
    """
    print("\n[3] the storage key sorts chronologically")
    check("hour 9 is padded to '-09'",
          hour_partition(datetime(2026, 9, 7, 9, tzinfo=timezone.utc)) == "2026-09-07-09")
    check("hour 23 is unchanged",
          hour_partition(datetime(2026, 9, 7, 23, tzinfo=timezone.utc)) == "2026-09-07-23")

    day = [datetime(2026, 9, 7, h, tzinfo=timezone.utc) for h in range(24)]
    padded = [hour_partition(d) for d in day]
    check("a full day sorts into clock order", sorted(padded) == padded)

    # The actual failure, stated as a test: on a complete day the unpadded max
    # is hour 9, so a max(event_hour) watermark skips 14 hours of data.
    unpadded = [hour_key(d) for d in day]
    check("unpadded max really is hour 9, not 23",
          max(unpadded) == "2026-09-07-9" and max(padded) == "2026-09-07-23",
          f"unpadded max={max(unpadded)}, padded max={max(padded)}")

    # The two formats must stay different things: the URL one must never pad,
    # or the download 404s.
    check("hour_key stays unpadded for the URL",
          hour_key(datetime(2026, 9, 7, 9, tzinfo=timezone.utc)) == "2026-09-07-9")


def test_completeness(types_df):
    """
    2026-09-05-10 is a real partial hour: valid file, zero parse errors, but
    no PushEvent/CreateEvent/DeleteEvent and roughly half the events of a
    healthy neighbouring hour. Nothing else in the pipeline catches that.
    """
    print("\n[4] partial hours are flagged")
    warns = completeness_warnings(types_df)
    check("the known-partial hour raises a warning", len(warns) > 0,
          warns[0][:64] if warns else "none")
    healthy = pd.DataFrame({
        "event_type": ["PushEvent", "CreateEvent", "DeleteEvent",
                       "PullRequestEvent", "IssuesEvent", "WatchEvent"],
        "events": [37148, 1465, 589, 27438, 11895, 6524],
    })
    check("a complete hour raises nothing", completeness_warnings(healthy) == [])


def test_conservation(df, types_df, repos_df):
    """Aggregates must account for every usable event -- no silent drops."""
    print("\n[5] aggregates conserve the input")
    total_in = len(df)
    check("event-type counts sum to input",
          int(types_df["events"].sum()) == total_in,
          f"{int(types_df['events'].sum()):,} vs {total_in:,}")
    check("repo counts sum to input",
          int(repos_df["events"].sum()) == total_in,
          f"{int(repos_df['events'].sum()):,} vs {total_in:,}")
    check("distinct repos matches raw",
          int(repos_df["repo_name"].nunique()) == int(df["repo_name"].nunique()),
          f"{repos_df['repo_name'].nunique():,} repos")
    # Cross-check one derived signal against the raw rows it came from.
    stars_raw = int((df["event_type"] == "WatchEvent").sum())
    check("stars_gained equals WatchEvent count",
          int(repos_df["stars_gained"].sum()) == stars_raw, f"{stars_raw:,} stars")


def test_idempotency(types_df, repos_df):
    """
    Load the same hour three times; the table must look identical to loading
    it once. This is the property Airflow retries depend on.
    """
    print("\n[6] loading the same hour repeatedly is idempotent")
    db = os.path.join(HERE, "test_idempotency.db")
    if os.path.exists(db):
        os.remove(db)
    conn = sqlite3.connect(db)
    ensure_schema(conn)

    snapshots = []
    for _ in range(3):
        load_partition(conn, types_df, EVENT_TYPE_TABLE, TEST_HOUR_KEY)
        load_partition(conn, repos_df, REPO_ACTIVITY_TABLE, TEST_HOUR_KEY)
        snapshots.append((
            pd.read_sql_query(f"SELECT * FROM {EVENT_TYPE_TABLE} ORDER BY event_type", conn),
            pd.read_sql_query(f"SELECT * FROM {REPO_ACTIVITY_TABLE} ORDER BY repo_name", conn),
        ))

    first_t, first_r = snapshots[0]
    check("event-type rows stable across 3 loads",
          all(s[0].equals(first_t) for s in snapshots), f"{len(first_t)} rows")
    check("repo rows stable across 3 loads",
          all(s[1].equals(first_r) for s in snapshots), f"{len(first_r):,} rows")
    check("no duplicate (hour, type) keys",
          not first_t.duplicated(["event_hour", "event_type"]).any())
    check("no duplicate (hour, repo) keys",
          not first_r.duplicated(["event_hour", "repo_name"]).any())

    # A second, different hour must coexist rather than replace the first.
    other = types_df.copy()
    other["event_hour"] = "2026-09-05-11"
    load_partition(conn, other, EVENT_TYPE_TABLE, "2026-09-05-11")
    hours = pd.read_sql_query(
        f"SELECT DISTINCT event_hour FROM {EVENT_TYPE_TABLE} ORDER BY event_hour", conn)
    check("loading another hour leaves the first intact",
          list(hours["event_hour"]) == ["2026-09-05-10", "2026-09-05-11"])
    conn.close()
    os.remove(db)


def test_truncated_file_is_loud():
    """
    A short download must fail, never yield a partial hour.

    This wedged the real backfill: 2026-09-08-6 arrived 19,101 bytes short of
    its declared 15,087,597, the read loop ended normally (a cut connection and
    a clean finish both look like read() == b''), and os.replace() promoted the
    truncated file. The size>0 cache check then served it to every retry, so
    retrying could never win.

    The property under test is that truncation is LOUD. Half an hour of events
    that parses without complaint is the failure mode this pipeline exists to
    refuse -- it is exactly the silent-partial-data problem completeness_
    warnings() was written for, one layer lower down.
    """
    print("\n[7] a truncated file fails loudly")
    import gzip as _gzip
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        good = os.path.join(td, "good.json.gz")
        with _gzip.open(good, "wb") as fh:
            for i in range(500):
                fh.write(b'{"type":"PushEvent","created_at":"2026-09-07T09:00:00Z",'
                         b'"repo":{"name":"o/r"},"actor":{"login":"a"},"payload":{}}\n')

        whole = open(good, "rb").read()
        cut = os.path.join(td, "cut.json.gz")
        with open(cut, "wb") as fh:                  # lop off the tail, as a short read does
            fh.write(whole[: int(len(whole) * 0.6)])

        df_ok, _ = read_hour_frame(good)
        check("the intact file reads", len(df_ok) == 500, f"{len(df_ok)} rows")

        try:
            df_bad, _ = read_hour_frame(cut)
        except EOFError:
            check("the truncated file raises, not returns partial data", True)
        except Exception as exc:
            check("the truncated file raises, not returns partial data", False,
                  f"raised {type(exc).__name__}, wanted EOFError")
        else:
            check("the truncated file raises, not returns partial data", False,
                  f"silently returned {len(df_bad)} rows -- this is the dangerous case")

    check("IncompleteDownload is distinct from HourNotAvailable",
          not issubclass(IncompleteDownload, HourNotAvailable))


def test_missing_hour():
    """GH Archive really is missing 2016-10-21-18; that must be catchable."""
    print("\n[8] a missing hour raises HourNotAvailable")
    try:
        download_hour(MISSING_HOUR, os.path.join(DATA_DIR, "_missing_probe"))
        check("404 raises HourNotAvailable", False, "no exception raised")
    except HourNotAvailable:
        check("404 raises HourNotAvailable", True)
    except Exception as exc:  # network down, DNS, etc -- don't fail the suite on that
        check("404 raises HourNotAvailable", False,
              f"unexpected {type(exc).__name__}: {exc} (network issue?)")


def main():
    path = os.path.join(DATA_DIR, f"{TEST_HOUR_KEY}.json.gz")
    if not os.path.exists(path):
        print(f"downloading {TEST_HOUR_KEY} (~73 MB, one time)...")
        path = download_hour(TEST_HOUR, DATA_DIR)

    df, stats = read_hour_frame(path)
    print(f"hour {TEST_HOUR_KEY}: {stats.total_lines:,} lines, {stats.usable:,} usable, "
          f"{stats.rejected} rejected, {stats.unparseable} unparseable")
    types_df = transform_event_types(df, TEST_HOUR_KEY)
    repos_df = transform_repo_activity(df, TEST_HOUR_KEY)

    test_dirty_data()
    test_hour_key_format()
    test_hour_partition_sorts()
    test_completeness(types_df)
    test_conservation(df, types_df, repos_df)
    test_idempotency(types_df, repos_df)
    test_truncated_file_is_loud()
    test_missing_hour()

    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("FAILED:", ", ".join(failed))
        sys.exit(1)
    print("All properties hold: idempotent, conserving, dirty-data tolerant, gap tolerant.")


if __name__ == "__main__":
    main()
