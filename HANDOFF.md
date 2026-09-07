# Handoff: data-engineering learning track

Context for picking this repo up in a new session.

## Who / why

Parsa Kiaee — 5+ years in telecom billing/operations (Irancell Labs,
Tecnotree, Pars Tasmim), moving into data engineering. Learning the DE
toolchain hands-on through real projects, specifically so resume claims stay
true: **nothing goes on the resume until it has actually been built and run.**

Also runs **Uniquizitor** (uniquizitor.com) — a Django/PostgreSQL exam
platform on self-hosted Docker with MinIO. It is *not* the data source for
this work; it doesn't have the volume to make a pipeline meaningful.

## Environment facts that cost time to rediscover

- Git host is **hamgit.ir** (`kiaee7`), credentials already stored. GitHub is
  reachable but the local SSH key is not registered there.
- **Network filtering matters.** `books.toscrape.com` and `api.binance.com`
  time out entirely from here. GH Archive, Wikimedia EventStreams and dumps,
  NYC TLC, Open-Meteo, OpenSky, CoinGecko, api.github.com, huggingface.co and
  archive.org all work. Test reachability before designing around a source.
- Host port **8080 is taken** by the `cute-site` container; Airflow uses 8081.
- Local Python is 3.14, so old pinned wheels (e.g. pandas 2.1.4) won't install.
  The venv at `.venv/` has pandas 3.0.5 — the same version Airflow 3.3.1 pins.
- Docker works fine: 20 CPUs, ~6 GB RAM free.

## Current project: `gharchive-pipeline/`

Airflow 3.3.1 hourly ETL over GH Archive. See its README for detail.

**Verified, actually run:** `docker compose up -d` with all services healthy,
one full DAG run to `state=success` loading 114,005 events / 44,924 repos into
Postgres, and 19/19 standalone logic checks.

**Not yet done:** never left running to backfill continuously; no streaming
layer; raw files sit in a Docker volume rather than MinIO.

## History worth knowing

Two earlier starter projects were deleted at commit `cb99250` and remain in
history at `e767346`:

- `exam-analytics-pipeline/` — Airflow + Kafka over Uniquizitor data. Ran only
  on synthetic data. Its **batch-vs-streaming reconciliation test** is the idea
  worth carrying forward: it replayed every attempt as an event and proved the
  streaming aggregates matched the batch aggregates exactly.
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
