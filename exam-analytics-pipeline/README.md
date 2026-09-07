# Exam Analytics Pipeline

A small, real Airflow pipeline that turns your exam platform's attempt data
into two useful analytics tables:

- **`analytics_subject_performance`** — pass rate and average score per subject
- **`analytics_question_difficulty`** — which questions get missed most often

It was built and tested against synthetic data first (`scripts/generate_sample_data.py`
+ `scripts/pipeline_functions.py`, no Airflow required) so the actual
extract/transform/load logic is proven correct before wrapping it in
orchestration. Confirmed output from that test run:

```
--- Subject performance (lowest pass rate first) ---
 subject_id      subject_name  attempts  avg_score  pass_rate
          5 General Knowledge       135       52.2       39.3
          4 Logical Reasoning       157       55.0       47.8
          2         Chemistry       171       61.5       66.7
          3           Physics       164       64.9       77.4
          1           Biology       173       73.7       92.5

--- Hardest questions (top 5 by miss rate) ---
     subject_name                    question_text  times_answered  miss_rate
        Chemistry Sample question 13 for subject 2             171       87.7
General Knowledge  Sample question 2 for subject 5             135       85.2
```

## Why this project (not a toy dataset)

It's your own exam platform's schema, so every design decision (what
"pass rate" means, how "difficulty" is computed, how to handle a question
with too few answers to be statistically meaningful) is one you can defend
in an interview, because you made the call and know why.

## Structure

```
docker-compose.yml           Airflow (LocalExecutor) + its own Postgres backend
requirements.txt             for running scripts/ outside Docker, if you want
dags/exam_analytics_dag.py   the Airflow DAG — 4 tasks: extract, 2x transform, load
scripts/pipeline_functions.py  extract/transform/load logic (used by both the DAG and local testing)
scripts/generate_sample_data.py  synthetic data generator (SQLite) for local testing
```

## Run it for real (on your WSL2/Docker setup, not in this chat)

1. Copy this whole folder to your machine.
2. In `docker-compose.yml`, fill in the four `PIPELINE_DB_*` values under
   `x-airflow-common` with your **real Django Postgres** connection details
   (the exam-system app's own database — not `airflow-db`, which is
   Airflow's internal metadata store and is separate on purpose).
3. Your Django schema uses different table names than the sample data
   (`question_bank_question`, `question_bank_subject`,
   `attempts_examattempt`, `attempts_answer`, etc., per your app's actual
   models). Update the SQL in `extract_answers()` in
   `scripts/pipeline_functions.py` to match — the join logic stays the
   same, just the table/column names change.
4. `docker-compose up -d`
5. Open `http://localhost:8080` (user: `admin`, password: `admin` — change
   this if it'll be reachable beyond your own machine), unpause
   `exam_analytics_pipeline`, and trigger a manual run.
6. Check the two `analytics_*` tables in your database once it finishes.

## Test the logic locally first (no Docker needed)

```bash
cd scripts
python3 generate_sample_data.py      # creates sample_exam_data.db
python3 pipeline_functions.py        # runs the full pipeline against it, prints results
```

## Once this is running for real

At that point "Apache Airflow" and "orchestration" are honest additions to
your resume — say so and I'll update it.

## Kafka: the speed layer

Alongside the nightly Airflow batch job, `kafka_producer.py` /
`kafka_consumer.py` add a near-real-time path for the same question (subject
pass rates), using **the same subject-performance logic**, just fed
one event at a time instead of via a batch groupby:

```
scripts/event_processing.py       pure incremental stats logic (no Kafka import -- testable without a broker)
scripts/kafka_producer.py         publishes one event per submitted attempt
scripts/kafka_consumer.py         consumes events, keeps live_subject_performance current
scripts/test_event_processing.py  proves the streaming math matches the batch pipeline exactly
```

**Already verified, no broker needed:** `test_event_processing.py` replays
every attempt from the sample data as a simulated event stream and confirms
the streaming result matches the batch pipeline's numbers exactly:

```
Replayed 800 attempt events through the streaming processor.
MATCH: streaming and batch agree exactly on every subject.
```

That's the real engineering question a streaming layer has to answer —
does the fast path agree with the batch path — answered and proven before
a single line of Kafka transport code had to be trusted.

### Running it for real

1. `docker-compose up -d kafka` (added alongside Airflow/Postgres above)
2. `pip install kafka-python` (or use `requirements.txt`)
3. In one terminal: `python3 scripts/kafka_consumer.py` — it'll sit and
   listen on the `exam-attempts` topic, refreshing `live_subject_performance`
   every 20 events.
4. In another terminal: `python3 scripts/kafka_producer.py` — publishes a
   handful of demo events so you can watch the consumer pick them up.
5. Wire `publish_attempt_event(...)` into your real Django app (the
   attempt-submission view, right after scoring) to make it real instead of
   demo events.

Once you've actually run producer → broker → consumer and watched
`live_subject_performance` update, Kafka becomes an honest resume line too
— same rule as Airflow: real run first, then it goes on the page.

### Next phase, if you want it: ClickHouse

Swap `live_subject_performance` (and the batch `analytics_*` tables) from
Postgres into ClickHouse or DuckDB — a columnar store built for exactly this
kind of aggregate query — and compare query time on the same analysis.
