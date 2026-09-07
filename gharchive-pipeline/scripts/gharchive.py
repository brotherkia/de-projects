"""
Core logic for the GH Archive pipeline: fetch one hour of GitHub events,
validate it, and aggregate it into two analytics tables.

Deliberately has ZERO Airflow imports. Everything here runs from a plain
`python3 gharchive.py`, which means the extract/transform/load logic can be
proven correct before any container exists -- the DAG is then a thin wrapper
that only handles scheduling and retries.

Data source: https://data.gharchive.org/YYYY-MM-DD-H.json.gz
One gzipped JSON-lines file per hour, every public GitHub event, back to 2011.
Measured on 2026-09-05-10: 73.2 MB gzipped, 363 MB raw, 71,829 events,
24,981 distinct repos, 20,024 distinct actors. So roughly 1.7M events/day.
"""
import gzip
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

BASE_URL = "https://data.gharchive.org"
USER_AGENT = "gharchive-learning-pipeline/1.0 (personal DE study project)"

# Tables this pipeline owns. Named so it's obvious they're derived, not raw.
EVENT_TYPE_TABLE = "gh_event_type_hourly"
REPO_ACTIVITY_TABLE = "gh_repo_activity_hourly"


class HourNotAvailable(Exception):
    """
    GH Archive is missing this hour (HTTP 404).

    This is NOT hypothetical: 2016-10-21-18 is genuinely absent from the
    archive -- the hour of the Dyn DDoS attack, when GitHub's own event feed
    was disrupted. A pipeline that treats a missing hour as a hard failure
    wedges its own backfill on the first gap, so callers are expected to catch
    this and skip the partition rather than retry it forever.
    """


@dataclass
class HourStats:
    """What happened while reading one hour -- for logging and data-quality checks."""
    total_lines: int = 0
    parsed: int = 0
    unparseable: int = 0   # not valid JSON
    rejected: int = 0      # valid JSON but missing fields we require

    @property
    def usable(self) -> int:
        return self.parsed - self.rejected


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def hour_key(dt: datetime) -> str:
    """
    '2026-09-05-10' -- GH Archive's hour naming.

    The hour is NOT zero-padded: hour 3 is '...-3', not '...-03'. Padding it
    yields a URL that 404s while looking perfectly reasonable, which is an
    easy way to mistake your own bug for a hole in the archive.
    """
    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}-{dt.hour}"


def hour_url(dt: datetime) -> str:
    return f"{BASE_URL}/{hour_key(dt)}.json.gz"


def download_hour(dt: datetime, dest_dir: str, timeout: int = 300) -> str:
    """
    Download one hour's file to dest_dir, returning the local path.

    Skips the download if the file is already there and non-empty, which is
    what makes re-running a task cheap: a retry after a downstream failure
    doesn't re-pull 73 MB.
    """
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, f"{hour_key(dt)}.json.gz")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    req = Request(hour_url(dt), headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp:
            tmp = path + ".partial"
            with open(tmp, "wb") as f:
                while True:
                    chunk = resp.read(1 << 20)  # 1 MiB at a time; never load 73 MB into RAM
                    if not chunk:
                        break
                    f.write(chunk)
            # Rename only once the download finished, so an interrupted run can
            # never leave a truncated file that later looks complete and cached.
            os.replace(tmp, path)
    except HTTPError as exc:
        if exc.code == 404:
            raise HourNotAvailable(f"{hour_url(dt)} returned 404") from exc
        raise
    return path


def iter_events(path: str):
    """
    Yield (event_dict, stats) for one hour file, streaming line by line.

    A truncated or corrupt line is counted and skipped rather than killing the
    run -- one bad line out of ~72k shouldn't cost you the whole hour.
    """
    stats = HourStats()
    with gzip.open(path, "rb") as fh:
        for line in fh:
            stats.total_lines += 1
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                stats.unparseable += 1
                continue
            stats.parsed += 1
            yield event, stats


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

def is_usable(event: dict) -> bool:
    """
    Does this event carry the fields both transforms depend on?

    Real GH Archive data contains events with an empty `repo` object -- e.g. a
    ForkEvent at 2026-09-05T10:02:01Z with repo == {} and public == false.
    Roughly 1 in 5,000. Indexing straight into event["repo"]["name"] raises
    KeyError and kills the task, so bad rows get counted and dropped here
    instead of crashing the pipeline.
    """
    if not event.get("type") or not event.get("created_at"):
        return False
    repo = event.get("repo") or {}
    actor = event.get("actor") or {}
    return bool(repo.get("name")) and bool(actor.get("login"))


def read_hour_frame(path: str) -> tuple:
    """
    Read one hour file into a flat DataFrame of just the columns we aggregate.

    Returns (DataFrame, HourStats). Building narrow rows as we stream keeps
    memory flat: the hour's full nested payloads are 363 MB of JSON, the six
    columns we actually aggregate are a small fraction of that.
    """
    rows = []
    stats = HourStats()
    for event, stats in iter_events(path):
        if not is_usable(event):
            stats.rejected += 1
            continue
        payload = event.get("payload") or {}
        pr = payload.get("pull_request") or {}
        rows.append(
            {
                "event_type": event["type"],
                "repo_name": event["repo"]["name"],
                "actor_login": event["actor"]["login"],
                "created_at": event["created_at"],
                # PullRequestEvent carries action ('opened'/'closed') and, when
                # closed, whether it was actually merged -- that distinction is
                # what separates "PR throughput" from "PR churn".
                "pr_action": payload.get("action") if event["type"] == "PullRequestEvent" else None,
                "pr_merged": bool(pr.get("merged")) if event["type"] == "PullRequestEvent" else False,
            }
        )
    return pd.DataFrame(rows), stats


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform_event_types(df: pd.DataFrame, event_hour: str) -> pd.DataFrame:
    """Per event type for this hour: volume, and how many distinct actors/repos drove it."""
    if df.empty:
        return pd.DataFrame(
            columns=["event_hour", "event_type", "events", "distinct_actors", "distinct_repos"]
        )
    out = (
        df.groupby("event_type")
        .agg(
            events=("event_type", "size"),
            distinct_actors=("actor_login", "nunique"),
            distinct_repos=("repo_name", "nunique"),
        )
        .reset_index()
    )
    out.insert(0, "event_hour", event_hour)
    return out.sort_values("events", ascending=False).reset_index(drop=True)


def transform_repo_activity(df: pd.DataFrame, event_hour: str, top_n: int = None) -> pd.DataFrame:
    """
    Per repo for this hour: total events plus a few signals worth separating.

    Keeps every repo by default, and that default is deliberate. Measured on
    2026-09-05-10: 24,981 distinct repos, 56.7% of them with exactly one event
    (median = 1), and the 500 busiest account for only 25% of events. So a
    per-hour "top N" cap silently drops three quarters of the data, and worse,
    it breaks any multi-hour rollup -- a repo that's steadily active all week
    without ever cracking a single hour's top 500 disappears entirely. That's
    the kind of error nobody notices until a number gets questioned much later.

    25k rows/hour is trivial for Postgres, so completeness is nearly free here.
    top_n stays available for deliberately sampling a backfill, but it is not
    the default.
    """
    cols = ["event_hour", "repo_name", "events", "distinct_actors",
            "stars_gained", "forks", "prs_opened", "prs_merged"]
    if df.empty:
        return pd.DataFrame(columns=cols)

    work = df.assign(
        _star=(df["event_type"] == "WatchEvent"),           # WatchEvent is a star, not a watch
        _fork=(df["event_type"] == "ForkEvent"),
        _pr_open=(df["pr_action"] == "opened"),
        _pr_merged=df["pr_merged"].fillna(False),
    )
    out = (
        work.groupby("repo_name")
        .agg(
            events=("event_type", "size"),
            distinct_actors=("actor_login", "nunique"),
            stars_gained=("_star", "sum"),
            forks=("_fork", "sum"),
            prs_opened=("_pr_open", "sum"),
            prs_merged=("_pr_merged", "sum"),
        )
        .reset_index()
    )
    out.insert(0, "event_hour", event_hour)
    for c in ["stars_gained", "forks", "prs_opened", "prs_merged"]:
        out[c] = out[c].astype(int)
    out = out.sort_values("events", ascending=False)
    if top_n is not None:
        out = out.head(top_n)
    return out.reset_index(drop=True)[cols]


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------

# Event types that appear in every healthy hour. PushEvent alone is normally
# the single largest category (~33% of events).
CORE_EVENT_TYPES = ("PushEvent", "CreateEvent", "DeleteEvent", "PullRequestEvent",
                    "IssuesEvent", "WatchEvent")


def completeness_warnings(types_df: pd.DataFrame) -> list:
    """
    Flag an hour that parsed cleanly but is missing whole event categories.

    This is the failure mode that a 404 check and a JSON-validity check both
    miss, and it is not hypothetical: 2026-09-05-10 downloads as a normal
    73 MB file, parses with zero errors, and contains no PushEvent,
    CreateEvent or DeleteEvent at all -- 13 event types instead of the usual
    16, and 71,825 events where the neighbouring hour has 114,005. Every
    number derived from it looks perfectly reasonable and is wrong by half.

    Silent partial data is worse than an outage, because nothing alerts.
    """
    warnings = []
    present = set(types_df["event_type"]) if not types_df.empty else set()
    missing = [t for t in CORE_EVENT_TYPES if t not in present]
    if missing:
        warnings.append(
            f"missing core event types {missing} -- this hour is likely partial, "
            f"treat its numbers as unreliable"
        )
    total = int(types_df["events"].sum()) if not types_df.empty else 0
    if total and total < 20000:
        warnings.append(f"only {total:,} events -- unusually low for a full hour")
    return warnings


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def get_connection():
    """
    DB-API connection. SQLite locally, Postgres when PIPELINE_DB_HOST is set
    (which is how the Airflow containers get pointed at the real database).
    Same functions run against both -- only this changes.
    """
    host = os.environ.get("PIPELINE_DB_HOST")
    if host:
        import psycopg2

        return psycopg2.connect(
            host=host,
            port=int(os.environ.get("PIPELINE_DB_PORT", 5432)),
            dbname=os.environ["PIPELINE_DB_NAME"],
            user=os.environ["PIPELINE_DB_USER"],
            password=os.environ["PIPELINE_DB_PASSWORD"],
        )
    here = os.path.dirname(os.path.abspath(__file__))
    return sqlite3.connect(os.path.join(here, "gharchive_local.db"))


def _is_postgres(conn) -> bool:
    return conn.__class__.__module__.startswith("psycopg2")


def ensure_schema(conn) -> None:
    """
    Create the analytics tables if they don't exist.

    Written by hand rather than left to pandas' to_sql(if_exists='replace')
    on purpose: replace drops and recreates the whole table, which would throw
    away every other hour each time one hour loads. Explicit DDL plus a
    delete-by-partition load is what makes hourly runs composable.
    """
    ddl = [
        f"""
        CREATE TABLE IF NOT EXISTS {EVENT_TYPE_TABLE} (
            event_hour      TEXT    NOT NULL,
            event_type      TEXT    NOT NULL,
            events          INTEGER NOT NULL,
            distinct_actors INTEGER NOT NULL,
            distinct_repos  INTEGER NOT NULL,
            PRIMARY KEY (event_hour, event_type)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {REPO_ACTIVITY_TABLE} (
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
        """,
    ]
    cur = conn.cursor()
    for stmt in ddl:
        cur.execute(stmt)
    conn.commit()


def load_partition(conn, df: pd.DataFrame, table: str, event_hour: str) -> int:
    """
    Replace exactly one hour's rows in `table` -- delete then insert, in a
    single transaction.

    This is what makes the pipeline idempotent: running 2026-09-05-10 five
    times leaves the same rows as running it once. Without it, an Airflow
    retry (or a backfill overlapping a scheduled run) silently double-counts,
    and that class of bug is invisible until someone questions a number
    months later.
    """
    cur = conn.cursor()
    ph = "%s" if _is_postgres(conn) else "?"
    cur.execute(f"DELETE FROM {table} WHERE event_hour = {ph}", (event_hour,))
    if not df.empty:
        cols = list(df.columns)
        placeholders = ", ".join([ph] * len(cols))
        cur.executemany(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
            [tuple(r) for r in df.itertuples(index=False, name=None)],
        )
    conn.commit()
    return len(df)


# ---------------------------------------------------------------------------
# One hour, end to end
# ---------------------------------------------------------------------------

def run_hour(dt: datetime, data_dir: str, conn=None, top_n: int = None) -> dict:
    """Fetch -> validate -> transform -> load a single hour. Returns a run summary."""
    event_hour = hour_key(dt)
    own_conn = conn is None
    conn = conn or get_connection()
    try:
        path = download_hour(dt, data_dir)
        df, stats = read_hour_frame(path)
        types_df = transform_event_types(df, event_hour)
        repos_df = transform_repo_activity(df, event_hour, top_n=top_n)

        warnings = completeness_warnings(types_df)
        for w in warnings:
            print(f"WARNING [{event_hour}]: {w}")

        ensure_schema(conn)
        load_partition(conn, types_df, EVENT_TYPE_TABLE, event_hour)
        load_partition(conn, repos_df, REPO_ACTIVITY_TABLE, event_hour)
        return {
            "event_hour": event_hour,
            "warnings": warnings,
            "lines": stats.total_lines,
            "usable": stats.usable,
            "unparseable": stats.unparseable,
            "rejected": stats.rejected,
            "event_types": len(types_df),
            "repos_loaded": len(repos_df),
        }
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    import sys

    # Default to an hour that definitely exists rather than "now" -- GH Archive
    # publishes on a lag, so the current hour is never there yet.
    arg = sys.argv[1] if len(sys.argv) > 1 else "2026-09-05-10"
    y, m, d, h = (int(x) for x in arg.split("-"))
    dt = datetime(y, m, d, h, tzinfo=timezone.utc)

    here = os.path.dirname(os.path.abspath(__file__))
    summary = run_hour(dt, data_dir=os.path.join(here, "data"))
    print("Loaded hour:", summary)

    conn = get_connection()
    print(f"\n--- {EVENT_TYPE_TABLE} (top 8 by volume) ---")
    print(pd.read_sql_query(
        f"SELECT * FROM {EVENT_TYPE_TABLE} WHERE event_hour = '{summary['event_hour']}'"
        " ORDER BY events DESC LIMIT 8", conn).to_string(index=False))
    print(f"\n--- {REPO_ACTIVITY_TABLE} (top 8 by events) ---")
    print(pd.read_sql_query(
        f"SELECT repo_name, events, distinct_actors, stars_gained, prs_opened, prs_merged"
        f" FROM {REPO_ACTIVITY_TABLE} WHERE event_hour = '{summary['event_hour']}'"
        " ORDER BY events DESC LIMIT 8", conn).to_string(index=False))
    conn.close()
