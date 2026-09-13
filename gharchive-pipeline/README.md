# GH Archive Hourly Pipeline

An Airflow 3 pipeline that turns [GH Archive](https://www.gharchive.org/) —
every public GitHub event, one gzipped JSON file per hour, back to 2011 — into
two analytics tables in Postgres:

- **`gh_event_type_hourly`** — per hour, how many events of each type, and how
  many distinct actors and repos drove them
- **`gh_repo_activity_hourly`** — per hour per repo: events, distinct actors,
  stars gained, forks, PRs opened, PRs merged

Alongside it, a Kafka path replays an hour through a topic, aggregates it by a
deliberately different method into **`gh_repo_activity_streaming`**, and
reconciles that against the batch table repo by repo.

## Status

**Verified, actually run:**

- `docker compose up -d` brings up Airflow 3.3.1 (LocalExecutor) + two Postgres
  instances. All services report healthy.
- **A continuous 140-hour backfill completed.** The DAG was unpaused and the
  scheduler worked from 2026-09-06-12 to 2026-09-12-07 one hour at a time —
  **142 runs, 0 failed**, serialized by `max_active_runs=1`. It loaded
  **9,771,985 events** and **3,883,472 repo-hour rows** covering 1,730,954
  distinct repos. Raw archive on disk: 4.8 GB. One column of it is wrong —
  see *Known wrong* below.
- `scripts/test_gharchive.py` → **33/33 checks pass**, no Airflow or containers
  needed. Last run 2026-09-13.
- `migrations/001_pad_event_hour.sql` applied to the analytics DB. Verified
  after: 0 unpadded keys, `max(event_hour)` agrees with the true chronological
  max, distinct keys == distinct real hours (nothing duplicated across the two
  key formats), and per-hour event counts unchanged — no double-counting.
- **Batch and streaming agree exactly on `2026-09-12-06`.** 68,681 events
  produced in ~11 s, consumed and folded in ~27 s into 29,093 repo rows, and
  `test_reconcile.py` passed **15/15**: identical repo sets, and all six metrics
  identical repo by repo and in total.
- **Consumer lag, made visible.** `gh_consumer.py --mode follow --throttle 0.0015`
  joined a real consumer group, was assigned all 6 partitions, and drained lag
  from 67,045 to 49,146 at a steady ~590 msg/s against a ~667 msg/s cap.
  Committed offsets per partition were 11,106 / 11,925 / 11,631 / 11,397 /
  11,470 / 11,152 — summing to exactly 68,681 and spread evenly, which is the
  `repo_name` key hashing well rather than piling onto one partition.
- Kafka (one broker, KRaft) healthy ~15 s after start. Kafka UI reports the
  cluster online with 1 broker and both replay topics at 68,681 messages.
  pgAdmin answers on its port.

**Known wrong in the loaded data:**

- **`prs_merged` is still 0 in every backfilled hour except `2026-09-12-06`.**
  The extraction bug behind it is fixed (problem 7 below) and that one hour was
  reloaded — its total went from 0 to 6,422, exactly the raw count of
  `action='merged'` — but the rest of the backfill has not been re-run. Any
  `prs_merged` figure spanning other hours is currently false.

**Not yet done:**

- Re-run the backfilled hours so `prs_merged` is corrected.
- The Kafka path is run by hand from the host venv. It is not wired into Airflow.
- Topic `gh.events.v1` is stale: it holds a replay produced before the
  `prs_merged` fix. Later runs used `gh.events.v2`. The demo consumer groups
  are left in place so they can be inspected in Kafka UI.
- Raw files still sit in a Docker volume rather than MinIO.
- There is no Spark in this project and none is planned for now.

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
docker-compose.yml            Airflow 3.3.1 LocalExecutor + airflow-db + analytics-db,
                              plus kafka (KRaft), kafka-ui and pgadmin
dags/gharchive_hourly.py      the DAG -- scheduling, retries, skip-on-missing only
scripts/gharchive.py          all real logic; zero Airflow imports
scripts/test_gharchive.py     standalone checks -- no containers, loads into SQLite
scripts/gh_producer.py        replays one hour into a Kafka topic, keyed by repo
scripts/gh_consumer.py        folds it back up per repo (replay), or shows lag (follow)
scripts/test_reconcile.py     streaming vs batch, every repo and every column
migrations/*.sql              schema changes to the analytics DB, applied by hand
pgadmin/                      pre-registers analytics-db as a server in pgAdmin
```

`scripts/` has no Airflow dependency on purpose: the extract/transform/load
logic is provable on its own, so a failure in the container is a scheduling
problem, never a "does the maths work" problem.

## Run it

On a machine that has never seen this repo, from the repo root:

```bash
./setup.sh          # checks prerequisites, makes .venv, writes .env, runs the standalone checks
```

Then bring up the stack:

```bash
cd gharchive-pipeline
docker compose up -d
```

`setup.sh` has already written `.env` with freshly generated secrets. Only if
you skipped it, `cp .env.example .env` and fill in the two secrets it lists —
never after `setup.sh`, or the copy replaces the generated secrets with blanks
and compose refuses to start.

| UI       | default URL           | notes                                          |
|----------|-----------------------|------------------------------------------------|
| Airflow  | http://localhost:8080 | admin / admin                                  |
| Kafka UI | http://localhost:8082 | topics, partitions, messages, consumer lag     |
| pgAdmin  | http://localhost:8083 | analytics-db pre-registered from `pgadmin/`    |

Every host port is overridable in `.env` (`AIRFLOW_PORT`, `ANALYTICS_DB_PORT`,
`KAFKA_PORT`, `KAFKA_UI_PORT`, `PGADMIN_PORT`) for machines where one is taken.

Then either unpause `gharchive_hourly` in the Airflow UI to let it backfill hour
by hour, or run a single hour synchronously:

```bash
docker compose exec airflow-scheduler \
  airflow dags test gharchive_hourly 2026-09-06T12:00:00+00:00

docker compose exec analytics-db psql -U analytics -d analytics \
  -c "SELECT event_type, events FROM gh_event_type_hourly ORDER BY events DESC;"
```

Test the logic with no containers at all, from the repo root:

```bash
.venv/bin/python gharchive-pipeline/scripts/test_gharchive.py
```

Call the venv's Python by a path without `..` in it. On Python 3.14 a relative
`../../.venv/bin/python` still runs, but prints a `sys.prefix` RuntimeWarning
on every start — which is why the Kafka commands below resolve it absolutely.

### Replay an hour through Kafka and reconcile it

These scripts run from the host venv, not inside Airflow. Two things about them
are easy to get wrong:

- **Point them at Postgres.** Without `PIPELINE_DB_HOST`, `get_connection()`
  falls back to a local SQLite file without saying so — the consumer still
  "succeeds", and the reconciliation reads the wrong database.
- **Use a fresh topic for each replay.** The consumer folds *every* message in
  the topic that belongs to the requested hour, with no de-duplication, so
  producing the same hour into one topic twice counts it twice. Neither script
  creates topics; create each one explicitly, with 6 partitions as the measured
  runs used.

```bash
cd gharchive-pipeline
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic gh.events.v3 --partitions 6 --replication-factor 1

cd scripts
export GH_TOPIC=gh.events.v3
# PIPELINE_DB_PORT must match ANALYTICS_DB_PORT in .env. If KAFKA_PORT is not
# 29092, also set KAFKA_BOOTSTRAP=localhost:<that port>.
export PIPELINE_DB_HOST=localhost PIPELINE_DB_PORT=5432 PIPELINE_DB_NAME=analytics \
       PIPELINE_DB_USER=analytics PIPELINE_DB_PASSWORD=analytics
PY="$(cd ../.. && pwd)/.venv/bin/python"

$PY gh_producer.py 2026-09-12-06      # the hour must already be in the batch tables
$PY gh_consumer.py 2026-09-12-06
$PY test_reconcile.py 2026-09-12-06   # exits non-zero on any mismatch
```

To watch consumer lag build and drain, join a real group while (or after) a
producer runs, then open Kafka UI → Consumers:

```bash
$PY gh_consumer.py --mode follow --group gh-live --throttle 0.0015
```

Stop it with Ctrl+C, which closes the consumer and leaves the group cleanly; it
also stops by itself after 20 s with nothing new. Don't stop it with `timeout`:
SIGTERM is not caught, so the killed member stays in the group until its 45 s
session timeout and blocks the next run's rebalance.

## Seven real problems this data forced

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

**7. A metric pinned at zero, which no check noticed.** `prs_merged` was 0 in all
140 backfilled hours — 3,883,472 rows — while `prs_opened` totalled 576,746.
Measured on `2026-09-12-06`: of 19,539 `PullRequestEvent`s, `action` is
`'merged'` on 6,422, and `pull_request.merged` is `None` on every one of the
19,539. So `bool(pr.get("merged"))` was never true. A metric stuck at zero raises
no error and drops no rows; it is simply untrue.

`is_merged_pr()` now accepts both shapes: `action='merged'` outright, and
`action='closed'` with `pull_request.merged` true, which is how GitHub's webhook
historically reported a merge. Handling only the new shape would silently
re-zero older hours.

The uncomfortable part: the reconciliation passed 15/15 **both before and after**
the fix. Batch and streaming share the field extraction, so they agreed while
both were wrong. Reconciliation proves two aggregation *methods* agree; it says
nothing about whether the fields feeding them were extracted correctly.

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

### The streaming path

**Two methods, one answer.** `gharchive.py` aggregates an hour with a pandas
groupby over the whole frame; `gh_consumer.py` keeps a running counter per repo
and never holds the hour at all. Two independent implementations over
byte-identical input must agree on every repo and every column, which is a far
stronger claim than "the streaming job ran without errors". This is the idea
carried forward from the deleted `exam-analytics-pipeline`.

**Messages are keyed by `repo_name`.** Kafka orders only within a partition and
routes by hash(key), so keying on the unit of aggregation puts every event for a
repo on one partition, and a consumer can fold it with no cross-partition
coordination. Key on event id instead and one consumer still gets the counts
right, but in a scaled-out group each consumer holds a fragment of every repo
and needs a shuffle to finish.

**At-least-once delivery, effectively-once results.** Kafka redelivers on
rebalance and restart. The consumer loads with the same delete-then-insert
`load_partition()` the batch path uses, and that idempotent load is what makes
redelivery harmless.

**Replay assigns; follow subscribes.** Replay `assign()`s every partition from
offset 0 and never commits, so a run cannot depend on where a previous one
stopped. The cost is that the broker does not know it exists — no group, no
committed offsets, no lag. Verified: `kafka-consumer-groups.sh --list` returned
empty after 140 hours of batch work and a full replay. Follow mode `subscribe()`s
with a real group and commits explicitly after processing, never on a timer:
auto-commit can acknowledge a message the consumer then crashes before handling.

**Monitoring must not cost what it monitors.** The first follow mode computed
lag with `get_watermark_offsets(cached=False)` and `committed()` — twelve blocking
round trips every three seconds across six partitions — and measured throughput
collapsed from 134 messages per tick to 1. It now reads the high-water mark the
broker already piggybacks on each fetch, plus the client's local `position()`:
the same number, with no network.

## Next

- Re-run the backfill so `prs_merged` is right in every hour, not just
  `2026-09-12-06`
- Wire the Kafka replay and reconciliation into Airflow, so every hour is
  reconciled rather than one hour by hand
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
