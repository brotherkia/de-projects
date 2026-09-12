# GH Archive Hourly Pipeline

An Airflow 3 pipeline that turns [GH Archive](https://www.gharchive.org/) —
every public GitHub event, one gzipped JSON file per hour, back to 2011 — into
two analytics tables in Postgres:

- **`gh_event_type_hourly`** — per hour, how many events of each type, and how
  many distinct actors and repos drove them
- **`gh_repo_activity_hourly`** — per hour per repo: events, distinct actors,
  stars gained, forks, PRs opened, PRs merged

## Status

**Verified, actually run:**

- `docker compose up -d` brings up Airflow 3.3.1 (LocalExecutor) + two Postgres
  instances. All services report healthy.
- **A continuous 140-hour backfill completed.** The DAG was unpaused and the
  scheduler worked from 2026-09-06-12 to 2026-09-12-07 one hour at a time —
  **142 runs, 0 failed**, serialized by `max_active_runs=1`. It loaded
  **9,771,985 events** and **3,883,472 repo-hour rows** covering 1,730,954
  distinct repos. Raw archive on disk: 4.8 GB.
- `scripts/test_gharchive.py` → **27/27 checks pass**, no Airflow or containers
  needed.
- `migrations/001_pad_event_hour.sql` applied to the analytics DB. Verified
  after: 0 unpadded keys, `max(event_hour)` agrees with the true chronological
  max, distinct keys == distinct real hours (nothing duplicated across the two
  key formats), and per-hour event counts unchanged — no double-counting.

**Not yet done:** no streaming layer yet — Kafka reconciliation is the next
step. Raw files still sit in a Docker volume rather than MinIO. There is no
Spark in this project and none is planned for now.

## Why this data source

Chosen after testing reachability from this network, where a lot is blocked.
GH Archive resolves and downloads at ~6 MB/s here. Measured on `2026-09-05-10`:
73 MB gzipped, 363 MB raw, ~72k events, ~25k distinct repos. About 1.7M
events/day — enough volume that aggregation is a real problem rather than a
formality.

It also has the property that makes Airflow worth learning at all: **a decade
of history you can backfill.** `catchup=True` with an hourly schedule is not a
toy setting here, it's the whole point.

## Structure

```
docker-compose.yml            Airflow 3.3.1 LocalExecutor + airflow-db + analytics-db
dags/gharchive_hourly.py      the DAG -- scheduling, retries, skip-on-missing only
scripts/gharchive.py          all real logic; zero Airflow imports
scripts/test_gharchive.py     27 checks, runs standalone
migrations/*.sql              schema changes to the analytics DB, applied by hand
```

`scripts/` has no Airflow dependency on purpose: the extract/transform/load
logic is provable on its own, so a failure in the container is a scheduling
problem, never a "does the maths work" problem.

## Run it

On a machine that has never seen this repo, from the repo root:

```bash
./setup.sh          # checks prerequisites, makes .venv, runs the 19 checks
```

Then bring up Airflow:

```bash
cd gharchive-pipeline
cp .env.example .env      # then fill in the two secrets it lists
docker compose up -d
# UI at http://localhost:8080  (admin / admin)
```

Then either unpause `gharchive_hourly` in the UI to let it backfill hour by
hour, or run a single hour synchronously:

```bash
docker compose exec airflow-scheduler \
  airflow dags test gharchive_hourly 2026-09-06T12:00:00+00:00

docker compose exec analytics-db psql -U analytics -d analytics \
  -c "SELECT event_type, events FROM gh_event_type_hourly ORDER BY events DESC;"
```

Test the logic with no containers at all:

```bash
cd scripts && python3 test_gharchive.py
```

## Six real problems this data forced

None of these were invented for the exercise — each was hit while building.

**1. A malformed event in every hour.** Roughly 1 in 5,000 events has an empty
`repo: {}` object (a `ForkEvent` with `public: false`). `event["repo"]["name"]`
raises `KeyError` and kills the task. `is_usable()` counts and drops them.

**2. Genuinely missing hours.** `2016-10-21-18` returns 404 — the hour of the
Dyn DDoS attack. Treating that as a failure wedges the whole backfill behind it,
so it raises `HourNotAvailable` → `AirflowSkipException`.

**3. The URL trap.** GH Archive does *not* zero-pad the hour: `...-3`, not
`...-03`. Padding it produces a plausible URL that 404s, which is an easy way
to mistake your own bug for a hole in the archive. There's a test for it now,
because that mistake was actually made here.

**4. Silently partial hours — the dangerous one.** `2026-09-05-10` downloads as
a normal 73 MB file and parses with zero errors, but contains **no `PushEvent`,
`CreateEvent` or `DeleteEvent` at all** — 13 event types instead of 16, and
71,825 events where the next day's same-ish hour has 114,005. Every number
derived from it looks completely reasonable and is wrong by half.

That's the failure mode neither a 404 check nor a JSON-validity check catches,
and it's why `completeness_warnings()` exists. Silent partial data is worse
than an outage, because nothing alerts.

**5. A silently truncated download that retries could never fix.** The backfill
wedged on `2026-09-08-6`. The file arrived as 15,068,496 bytes against a declared
`content-length` of 15,087,597 — 19,101 short, 0.13% — and every `aggregate`
retry died on `EOFError: Compressed file ended before the end-of-stream marker`.

A cut connection and a finished response are indistinguishable to the read loop:
both end with `read()` returning `b''`. So the loop exited normally and
`os.replace()` promoted a truncated file to its final name. The `size > 0` cache
check then served that corpse to every retry, so retrying could never win — and
`max_active_runs=1` queued the whole backfill behind one 19 KB shortfall.
`download_hour()` now compares bytes written against `content-length` *before*
the rename, and raises `IncompleteDownload` rather than promoting a short file.

Worth recording: adding retry backoff first made this **worse**, stretching the
wedge from ~4 minutes to ~44. Backoff is the right answer for a transient
network fault and the wrong answer for a poisoned cache.

**6. The archive's URL format is not a sort order.** `event_hour` was stored in
GH Archive's own naming, where the hour is not zero-padded. Correct for the URL —
padding it 404s, see problem 3 — but as a TEXT primary key it sorts
`0, 1, 10, 11, … 19, 2, 20`. On any complete day the lexicographic max of hours
0..23 is `9`, so `max(event_hour)`, the obvious watermark query, reported 09:00
as the newest hour of a day running to 23:00.

Equality was never affected, which is why it hid for 40+ loaded hours: the loads
match with `WHERE event_hour = %s`, so idempotency held the whole time while
`ORDER BY` quietly lied. `hour_partition()` now supplies the padded storage key,
`hour_key()` is restricted to URLs and raw filenames, and
`migrations/001_pad_event_hour.sql` rewrites the rows already loaded.

## Design decisions worth defending

**Idempotent partition loads.** Every load is delete-then-insert scoped to one
`event_hour`, in one transaction. Airflow retries tasks, and backfills overlap
scheduled runs; without this, re-running an hour silently doubles its numbers.
Tested explicitly by loading the same hour three times and requiring identical
output.

**XCom carries paths, not data.** XCom rows live in Airflow's metadata database.
Pushing a 45k-row DataFrame through it bloats that database and slows every
scheduler query. Tasks write Parquet to a shared volume and pass the path.

**Two separate Postgres instances.** `airflow-db` holds Airflow's own metadata;
`analytics-db` holds this pipeline's output. They should never be one bad
migration away from each other.

**Keep every repo, no top-N cap.** An early version kept only the 500 busiest
repos per hour. Measured: that's **25% of events**, and 56.7% of repos have
exactly one event. Worse, a per-hour cap breaks multi-hour rollups — a repo
steadily active all week without ever cracking a single hour's top 500
disappears entirely. 25k rows/hour is nothing for Postgres; completeness is
nearly free.

**`data_interval_start`, never `datetime.now()`.** The hour a run processes
comes from the window Airflow assigned it. That's what makes a rerun months
later still process the hour it was created for.

## Next

- Kafka layer: replay an hour's events through a topic and reconcile the
  streaming aggregates against these batch tables — they must agree exactly.
  This is the idea carried forward from the deleted `exam-analytics-pipeline`,
  and it is the next thing to build.
- Move raw files to MinIO instead of a Docker volume
- DuckDB or ClickHouse for the analytics store, and compare query time

## A data-quality result worth keeping

Of the 140 hours backfilled, **25 arrived degraded** — 18% of a six-day window.
Every one of them is missing exactly the same three event types:

```
PushEvent, CreateEvent, DeleteEvent
```

That is the identical signature as `2026-09-05-10` (problem 4 above), so this is
a systematic upstream failure mode rather than random loss. Volumes in those
hours collapse from ~70–116k events to ~5–18k.

It is upstream, not a bug here, and that was checked rather than assumed: local
file sizes match the server's `content-length` byte for byte, every file passes
`gzip -t`, and raw line counts match loaded events exactly (87,562 lines →
87,563 rows; the +1 is the trailing newline). The pipeline loads precisely what
was published.

`completeness_warnings()` flagged all 25 unaided. It was written against a
sample of one; it has now proven itself on 25 more.
