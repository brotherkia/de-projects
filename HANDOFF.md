# Handoff: data-engineering learning track

Context for picking this repo up in a new session, on any machine.

## Start here on a new machine

```bash
git clone https://github.com/brotherkia/de-projects.git DE && cd DE && ./setup.sh
```

`setup.sh` checks prerequisites, builds `.venv`, and runs the standalone test
suite so you know the logic works there before Docker is involved. Then see
`gharchive-pipeline/README.md` to bring up Airflow.

Nothing else is needed: the repo carries no data (the pipeline re-downloads
what it needs) and no secrets at all. `setup.sh` generates the two Airflow
secrets into `gharchive-pipeline/.env`, which is gitignored; compose refuses
to start without them.

## Who / why

Parsa Kiaee — 5+ years in telecom billing/operations (Irancell Labs,
Tecnotree, Pars Tasmim), moving into data engineering. Learning the DE
toolchain hands-on through real projects, specifically so resume claims stay
true: **nothing goes on the resume until it has actually been built and run.**

Also runs **Uniquizitor** (uniquizitor.com) — a Django/PostgreSQL exam
platform on self-hosted Docker with MinIO. It is *not* the data source for
this work; it doesn't have the volume to make a pipeline meaningful.

## Environment facts that cost time to rediscover

- Git host for **this** repo is **GitHub** (`brotherkia/de-projects`, public),
  over HTTPS. `gh` lives at `~/.local/bin/gh` and is wired in as git's
  credential helper for github.com, so push just works; there is no SSH key.
  `cute-site` and `online-exam` still live on **hamgit.ir** (`kiaee7`) and
  still use the token in `~/.git-credentials` — don't delete that file.
- **TLS interception happens on this network.** Mid-session, github.com *and*
  hamgit.ir both started presenting certs issued by `CN=MTNISubCA01`
  (MTN Irancell) instead of their real CAs, and every HTTPS git operation
  failed verification. It cleared on its own. Never "fix" that by disabling
  `http.sslVerify` — that hands your token to whoever runs the proxy. Wait it
  out, or use a VPN.
- **Network filtering matters.** `books.toscrape.com` and `api.binance.com`
  time out entirely from here. GH Archive, Wikimedia EventStreams and dumps,
  NYC TLC, Open-Meteo, OpenSky, CoinGecko, api.github.com, huggingface.co and
  archive.org all work. Test reachability before designing around a source.
- On *this* machine host ports **8080 and 5432 are both taken** — 5432 by
  `online-exam-db-1` — so `.env` here sets `AIRFLOW_PORT=8081` and
  `ANALYTICS_DB_PORT=5433`, putting the UI on **localhost:8081**. Compose
  defaults to 8080/5432, which will fail to bind here. The other three stay on
  their defaults: Kafka UI **8082**, pgAdmin **8083**, broker **29092**. `.env`
  is gitignored because that is a fact about one machine — and because it also
  holds the two Airflow secrets.
- **The Kafka scripts run from the host venv and need `PIPELINE_DB_*` set**
  (`PIPELINE_DB_PORT=5433` here). Without `PIPELINE_DB_HOST`, `get_connection()`
  silently uses a local SQLite file instead of Postgres. The full invocation is
  in the pipeline README.
- **Stop `gh_consumer.py --mode follow` with Ctrl+C, not `timeout`.** SIGTERM
  isn't caught, so the killed member lingers in the group for its 45 s session
  timeout and blocks the next run's rebalance. That cost one confusing
  "partitions 0" result before the broker log explained it.
- Local Python is 3.14, so old pinned wheels (e.g. pandas 2.1.4) won't install.
  The venv at `.venv/` has pandas 3.0.5 — the same version Airflow 3.3.1 pins.
  **If `python3 -m venv` is interrupted it leaves a venv with no `pip` and an
  empty `site-packages`.** That happened here and made the test suite look
  broken on a machine where it had previously passed. `rm -rf .venv` and re-run
  `./setup.sh` rather than debugging imports.
- Docker Hub pulls work. **`docker manifest inspect` fails with a CloudFront 403
  regardless**, which looks exactly like a geo-block and is not one — it is an
  auth quirk of that subcommand. Test image availability with a real `docker
  pull`, never with `manifest inspect`.
- The Airflow services carry `restart: always`; the two Postgres containers do
  not. After a host reboot the stack comes back **half-up** — scheduler running,
  databases down. `docker compose up -d` fixes it.
- Docker works fine: 20 CPUs, ~6 GB RAM free.

## Current project: `gharchive-pipeline/`

Airflow 3.3.1 hourly ETL over GH Archive, plus a Kafka replay path that
reconciles against it. See its README for detail.

**Verified, actually run:** `docker compose up -d` with all services healthy;
a continuous **140-hour backfill** (2026-09-06-12 → 2026-09-12-07) of **142 runs,
0 failed**, loading **9,771,985 events** and 3,883,472 repo-hour rows over
1,730,954 distinct repos; 4.8 GB of raw archive on disk; **33/33** standalone
logic checks (last run 2026-09-13); and `migrations/001_pad_event_hour.sql`
applied and verified.

On the streaming side: a single-broker KRaft Kafka, with **batch and streaming
reconciling exactly on 2026-09-12-06** — 68,681 events, 29,093 repos,
**15/15** checks. Consumer lag was made visible with a throttled follow-mode
consumer, and Kafka UI (8082) and pgAdmin (8083) were brought up for inspecting
the stream and the tables.

**Known wrong right now:** `prs_merged` is 0 in every backfilled hour except
2026-09-12-06. The bug is fixed and that hour was reloaded (0 → 6,422, matching
the raw `action='merged'` count), but the rest of the backfill has not been
re-run.

**Not yet done:** re-running the backfill to correct `prs_merged`. The Kafka path
is run by hand from the venv, not from Airflow. Raw files still sit in a Docker
volume rather than MinIO. **There is no Spark in this project** and none is
planned; if a resume ever says otherwise, it is wrong.

**Three bugs found and fixed around that backfill**, all documented in the
pipeline README:

- a silently truncated download (19,101 bytes short of `content-length`) that
  was promoted to final and then cached, so retries could never win — it wedged
  the whole backfill behind one hour
- `event_hour` stored in the archive's unpadded URL format, which sorts
  `0,1,10,...,19,2,20`, making `max(event_hour)` report hour 9 as the latest
  hour of a complete day. Equality was unaffected, so idempotency held and the
  bug hid for 40+ hours.
- `prs_merged` pinned at 0 across all 3,883,472 rows. In this data a merge is
  `action='merged'` with `pull_request.merged` None (measured on 2026-09-12-06),
  and the code only looked at `pull_request.merged`.
  The batch-vs-streaming reconciliation passed 15/15 *while it was wrong*: both
  paths share the field extraction, so they agreed on the wrong answer. Found by
  hand, not by any check.

## History worth knowing

**This repo moved from hamgit.ir to GitHub, and its history was rewritten on
the way.** `docker-compose.yml` had a real Fernet key and JWT secret committed
as literal values; publishing publicly would have made them permanent. The
three affected commits were rewritten, so those two values appear nowhere in
GitHub's history, and compose now reads both from `.env`. Consequence to know
about: the three commits from `gharchive-pipeline` onward have **different
hashes** than the ones still on hamgit.

hamgit.ir/kiaee7/de-projects still exists, untouched, with the original
secret-bearing history — a force-push there was rejected because `main` is a
protected branch. **Treat both old keys as burned.** If either was ever used
against an Airflow instance holding real connection credentials, rotate those
credentials: the Fernet key is what decrypts them.

Two earlier starter projects were deleted at commit `cb99250` and remain in
history at `e767346`:

- `exam-analytics-pipeline/` — Airflow + Kafka over Uniquizitor data. Ran only
  on synthetic data. Its **batch-vs-streaming reconciliation test** was the idea
  worth carrying forward — it replayed every attempt as an event and proved the
  streaming aggregates matched the batch aggregates exactly — and it now lives
  on as `gharchive-pipeline/scripts/test_reconcile.py`, against real data.
- `book-scraper/` — dead on arrival, target site unreachable from this network.

## How to keep working on this

- Explain the concept alongside the code, tied to this project — not abstract
  examples.
- Prove logic locally before trusting it against anything live. Every
  non-obvious claim in this repo has a measurement or a test behind it.
- Be explicit about verified vs. merely written. Don't blur "I wrote code for
  this" into "this works."
- Measure before asserting. Several confident guesses made while building this
  turned out wrong — per-hour volume was off by 18x, a "missing" archive hour
  was a zero-padding bug, and a top-500 cap silently discarded 75% of events.
  All three were caught by checking rather than reasoning.
- Resume claims follow real, run, working code — never the reverse.
