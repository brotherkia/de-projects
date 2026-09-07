# Handoff: data-engineering learning track

Context for picking this up in Claude Code / WSL. Drop this file at the
repo root so it's readable as context there too. This repo holds **two
separate projects** — don't let the folder names blur together, they
learn different parts of the toolchain.

## Who / why

Parsa Kiaee — 5+ years in telecom billing/operations (Irancell Labs,
Tecnotree, Pars Tasmim), transitioning into data engineering. Learning the
DE toolchain hands-on through real personal projects specifically so resume
claims stay true — nothing gets added to the resume until it's actually
been built and run, not just planned.

Existing platform: **Uniquizitor** — a Django/PostgreSQL online exam
platform, live at uniquizitor.com, using self-hosted Docker infra and MinIO
for object storage. Project 1 below builds on top of its data; project 2
is unrelated to it (a separate skill: data acquisition via scraping).

## Project 1: `exam-analytics-pipeline/` — orchestration + streaming

Computes per-subject pass rates and question-difficulty (miss rate) from
Uniquizitor's exam-attempt data, two ways:

- **Batch layer**: Airflow DAG, 4 tasks (extract → [transform pass-rates,
  transform difficulty] in parallel → load), nightly schedule, retries.
  `dags/exam_analytics_dag.py`, logic in `scripts/pipeline_functions.py`.
- **Streaming layer**: Kafka producer/consumer computing the same
  pass-rate numbers incrementally, one event per attempt.
  `scripts/kafka_producer.py`, `scripts/kafka_consumer.py`, core logic
  (no Kafka dependency, independently testable) in
  `scripts/event_processing.py`.

**Verified, locally, without needing Airflow or a Kafka broker running:**
- ETL logic tested end-to-end against a full synthetic dataset
  (`scripts/generate_sample_data.py` + `scripts/pipeline_functions.py`) —
  produces sensible, correct subject pass-rates and difficulty rankings.
- The streaming logic was replay-tested against the batch logic on
  identical data (`scripts/test_event_processing.py`) — **they agree
  exactly**. This is the strongest talking point in the whole project:
  proving the fast path agrees with the batch/source-of-truth path is a
  real, senior-level data engineering concern, not a toy detail.

**NOT yet done:**
- Never run against the real Uniquizitor production database — only
  synthetic data shaped like the schema. `extract_answers()` in
  `pipeline_functions.py` needs its SQL updated to match the real Django
  table names (`question_bank_question`, `attempts_examattempt`, etc.).
- `docker-compose up` has never actually been run — Airflow and Kafka
  exist only as verified-correct code, not as something that's executed.
- Once both of those happen for real, the resume's "actively building"
  language for Airflow/Kafka should be tightened to a completed claim —
  ask for that update once it's true.

## Project 2: `book-scraper/` — data acquisition, new and separate

Target: `books.toscrape.com` (a site built for scraping practice —
avoids the ToS/legal gray area of scraping a real commercial site while
learning the mechanics). Has nothing to do with Uniquizitor or project 1
— this is a standalone skill-building project on its own data source.

**Verified:** `scripts/scrape_books.py` correctly parses title, price,
availability, and star rating from real page structure — tested against
`scripts/sample_page.html`, a saved sample built from real fetched data
(A Light in the Attic, £51.77, etc.), not fabricated data.

**NOT yet done:**
- Never actually fetched a live page — blocked in the chat sandbox
  (`x-deny-reason: host_not_allowed`), but should work fine from WSL.
- No pagination yet (site has 50 pages, ~1000 books total).
- No transform step (clean price strings to numbers, standardize
  categories) or load step (into Postgres) yet — currently extract-only.

## Immediate next steps, in order

1. `git init` the repo in WSL; commit both project folders as the
   starting point (as two folders in one repo, or split into two repos —
   either is fine, just keep them conceptually separate like this).
2. In `book-scraper/`: get `scrape_books.py` actually fetching live pages
   and loop through all 50 pages, with a delay between requests.
3. In `book-scraper/`: add transform (price string → float, category
   cleanup) and load (into Postgres) to complete a real ETL pipeline.
4. In `exam-analytics-pipeline/`: point `pipeline_functions.py` at the
   real Uniquizitor database and actually run the Airflow DAG + Kafka
   producer/consumer for real.

## How to keep mentoring this

- Explain the concept before or alongside the code, tied to the actual
  project — not abstract examples.
- Prove logic locally (tests, replay comparisons, saved samples) before
  trusting it against anything live — that pattern runs through both
  projects and should continue.
- Be explicit, always, about what's verified vs. still just written —
  don't blur "I wrote code for this" into "this works."
- Keep unrelated projects in unrelated folders — don't repeat today's mistake.
- Resume claims follow real, run, working code — never the reverse.
