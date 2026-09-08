# Handoff: data-engineering learning track

Context for picking this repo up in a new session, on any machine.

## Start here on a new machine

```bash
git clone https://github.com/brotherkia/de-projects.git DE && cd DE && ./setup.sh
```

`setup.sh` checks prerequisites, builds `.venv`, and runs the 19-check test
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
  defaults to 8080/5432, which will fail to bind here. `.env` is gitignored
  because that is a fact about one machine — and because it also holds the
  two Airflow secrets.
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
