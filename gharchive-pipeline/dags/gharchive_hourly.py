"""
Hourly GH Archive ETL.

One DAG run per hour of GitHub history: download that hour's event file,
aggregate it, and load two analytics tables. Written for Airflow 3.

Task shape -- download -> aggregate -> (load types | load repos):

    The two loads run in parallel because they're independent, and they read
    small pre-aggregated files rather than re-parsing 363 MB of JSON each.

    XCom carries FILE PATHS, never DataFrames. XCom rows live in Airflow's
    metadata database; pushing a 25k-row frame through it bloats that database
    and slows every scheduler query against it. The staging directory is a
    shared volume, so tasks hand each other paths and the data never enters
    Airflow's own storage.

The real logic lives in scripts/gharchive.py, which has no Airflow imports and
is tested standalone by scripts/test_gharchive.py. This file only handles
scheduling, retries, and what to do when an hour is missing.
"""
import os
import sys
from datetime import datetime, timezone

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

# scripts/ is mounted alongside dags/ -- see docker-compose.yml
sys.path.append("/opt/airflow/scripts")

from gharchive import (  # noqa: E402
    EVENT_TYPE_TABLE,
    REPO_ACTIVITY_TABLE,
    HourNotAvailable,
    completeness_warnings,
    download_hour,
    ensure_schema,
    get_connection,
    hour_key,
    load_partition,
    read_hour_frame,
    transform_event_types,
    transform_repo_activity,
)

DATA_DIR = os.environ.get("GH_DATA_DIR", "/opt/airflow/gh_data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STAGING_DIR = os.path.join(DATA_DIR, "staging")


@dag(
    dag_id="gharchive_hourly",
    description="Hourly GitHub event volume and per-repo activity from GH Archive",
    schedule="@hourly",
    # Fixed past start date + catchup=True means unpausing this DAG backfills
    # every hour from here to now, one run per hour. That is the point: it's
    # how you learn what catchup, max_active_runs and idempotency actually do.
    start_date=pendulum.datetime(2026, 9, 6, 12, tz="UTC"),
    catchup=True,
    # One hour at a time. Without this, unpausing fires ~25 runs at once, each
    # downloading 73 MB -- rude to a free public archive and hard on a laptop.
    max_active_runs=1,
    default_args={
        "owner": "parsa",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
    },
    tags=["gharchive", "hourly", "elt"],
)
def gharchive_hourly():

    @task
    def download(data_interval_start=None) -> str:
        """
        Fetch the hour this run is responsible for.

        The hour comes from data_interval_start -- the window Airflow assigned
        this run -- never from datetime.now(). That's what makes a rerun in
        November still process the September hour it was created for, and it's
        the difference between a backfillable pipeline and one that only ever
        looks at "right now".
        """
        dt = data_interval_start.in_timezone("UTC")
        hour = datetime(dt.year, dt.month, dt.day, dt.hour, tzinfo=timezone.utc)
        try:
            path = download_hour(hour, RAW_DIR)
        except HourNotAvailable as exc:
            # GH Archive publishes on a lag and has genuine holes in its
            # history (2016-10-21-18, for one). Skipping marks the run
            # complete-but-empty; failing would retry forever and, with
            # max_active_runs=1, block every later hour behind it.
            raise AirflowSkipException(f"hour not published: {exc}") from exc
        size_mb = os.path.getsize(path) / 1048576
        print(f"downloaded {hour_key(hour)} -> {path} ({size_mb:.1f} MB)")
        return path

    @task
    def aggregate(raw_path: str, data_interval_start=None) -> dict:
        """Parse the hour once, write both aggregates to staging as Parquet."""
        dt = data_interval_start.in_timezone("UTC")
        event_hour = hour_key(datetime(dt.year, dt.month, dt.day, dt.hour, tzinfo=timezone.utc))

        df, stats = read_hour_frame(raw_path)
        print(f"{event_hour}: {stats.total_lines:,} lines, {stats.usable:,} usable, "
              f"{stats.rejected} rejected, {stats.unparseable} unparseable")

        # A sudden spike in rejects means the upstream schema shifted under us.
        # Worth shouting about in the logs even though it isn't fatal.
        if stats.parsed and stats.rejected / stats.parsed > 0.01:
            print(f"WARNING: {100 * stats.rejected / stats.parsed:.2f}% of events rejected "
                  f"-- check whether GH Archive's schema changed")

        types_df = transform_event_types(df, event_hour)
        # An hour can download and parse perfectly while still being half
        # empty -- see completeness_warnings(). Loud in the logs, but not a
        # failure: the partial data is still the best record of that hour,
        # and failing here would just block the backfill behind it.
        for w in completeness_warnings(types_df):
            print(f"DATA QUALITY [{event_hour}]: {w}")

        os.makedirs(STAGING_DIR, exist_ok=True)
        out = {}
        for name, frame in [
            ("event_types", types_df),
            ("repo_activity", transform_repo_activity(df, event_hour)),
        ]:
            path = os.path.join(STAGING_DIR, f"{event_hour}.{name}.parquet")
            frame.to_parquet(path, index=False)
            out[name] = path
            print(f"staged {name}: {len(frame):,} rows -> {path}")
        out["event_hour"] = event_hour
        return out

    @task
    def load_event_types(staged: dict) -> int:
        return _load(staged["event_types"], EVENT_TYPE_TABLE, staged["event_hour"])

    @task
    def load_repo_activity(staged: dict) -> int:
        return _load(staged["repo_activity"], REPO_ACTIVITY_TABLE, staged["event_hour"])

    staged = aggregate(download())
    [load_event_types(staged), load_repo_activity(staged)]


def _load(parquet_path: str, table: str, event_hour: str) -> int:
    """Delete-then-insert this hour's rows, so retries and backfills don't double-count."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    conn = get_connection()
    try:
        ensure_schema(conn)
        n = load_partition(conn, df, table, event_hour)
    finally:
        conn.close()
    print(f"loaded {n:,} rows into {table} for {event_hour}")
    return n


gharchive_hourly()
