"""
Extract / Transform / Load logic for the exam-analytics pipeline.

Written against plain DB-API connections (sqlite3 locally, psycopg2 against
the real Postgres DB) so the exact same functions run in both places —
only get_connection() changes between environments.
"""
import io
import os
import sqlite3

import pandas as pd

# ---------------------------------------------------------------------------
# Serialization (used by the Airflow DAG to pass DataFrames through XCom)
# ---------------------------------------------------------------------------

def df_from_json(payload: str) -> pd.DataFrame:
    """
    Rebuild a DataFrame from a DataFrame.to_json() string.

    pandas 3 removed passing a JSON *string* straight to read_json() -- a bare
    string is now read as a file path, so the old call died with
    FileNotFoundError. StringIO is explicit about "this is data, not a path"
    and works on pandas 2 and 3 alike.

    Lives here rather than in the DAG so it can be tested without Airflow
    installed -- same reason event_processing.py has no Kafka import.
    """
    return pd.read_json(io.StringIO(payload))


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection():
    """
    Returns a DB-API connection.

    Local/dev default: the SQLite sample DB created by generate_sample_data.py.
    Production (set via env vars, e.g. in the Airflow DAG / docker-compose):
        PIPELINE_DB_HOST, PIPELINE_DB_NAME, PIPELINE_DB_USER, PIPELINE_DB_PASSWORD
    pointed at the real Django Postgres database.
    """
    host = os.environ.get("PIPELINE_DB_HOST")
    if host:
        import psycopg2  # only required in the real/Airflow environment

        return psycopg2.connect(
            host=host,
            dbname=os.environ["PIPELINE_DB_NAME"],
            user=os.environ["PIPELINE_DB_USER"],
            password=os.environ["PIPELINE_DB_PASSWORD"],
        )
    here = os.path.dirname(os.path.abspath(__file__))
    return sqlite3.connect(os.path.join(here, "sample_exam_data.db"))


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_answers(conn) -> pd.DataFrame:
    """One row per answer, joined with subject and question text."""
    query = """
        SELECT
            a.id            AS answer_id,
            a.attempt_id    AS attempt_id,
            a.question_id   AS question_id,
            a.is_correct    AS is_correct,
            q.subject_id    AS subject_id,
            q.text          AS question_text,
            s.name          AS subject_name,
            ea.user_id      AS user_id,
            ea.submitted_at AS submitted_at
        FROM answers a
        JOIN questions q      ON q.id = a.question_id
        JOIN subjects s       ON s.id = q.subject_id
        JOIN exam_attempts ea ON ea.id = a.attempt_id
    """
    return pd.read_sql_query(query, conn)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform_subject_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Pass rate and volume per subject. 'Pass' = attempt scored >=60% on that subject's questions."""
    per_attempt = (
        df.groupby(["attempt_id", "subject_id", "subject_name"])["is_correct"]
        .mean()
        .reset_index(name="attempt_score")
    )
    per_attempt["passed"] = per_attempt["attempt_score"] >= 0.6

    out = (
        per_attempt.groupby(["subject_id", "subject_name"])
        .agg(
            attempts=("attempt_id", "nunique"),
            avg_score=("attempt_score", "mean"),
            pass_rate=("passed", "mean"),
        )
        .reset_index()
        .sort_values("pass_rate")
    )
    out["avg_score"] = (out["avg_score"] * 100).round(1)
    out["pass_rate"] = (out["pass_rate"] * 100).round(1)
    return out


def transform_question_difficulty(df: pd.DataFrame, min_answers: int = 10) -> pd.DataFrame:
    """Miss rate per question -- the questions students get wrong most often."""
    out = (
        df.groupby(["question_id", "subject_name", "question_text"])
        .agg(times_answered=("answer_id", "count"), times_correct=("is_correct", "sum"))
        .reset_index()
    )
    out = out[out["times_answered"] >= min_answers].copy()
    out["miss_rate"] = (1 - out["times_correct"] / out["times_answered"]) * 100
    out["miss_rate"] = out["miss_rate"].round(1)
    return out.sort_values("miss_rate", ascending=False)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_dataframe(conn, df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()


# ---------------------------------------------------------------------------
# Full pipeline (what the Airflow tasks call)
# ---------------------------------------------------------------------------

def run_pipeline():
    conn = get_connection()
    try:
        raw = extract_answers(conn)
        subject_perf = transform_subject_performance(raw)
        question_diff = transform_question_difficulty(raw)
        load_dataframe(conn, subject_perf, "analytics_subject_performance")
        load_dataframe(conn, question_diff, "analytics_question_difficulty")
        return {
            "answers_processed": len(raw),
            "subjects": len(subject_perf),
            "questions_flagged": len(question_diff),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    result = run_pipeline()
    print("Pipeline run complete:", result)

    # quick peek at the output
    conn = get_connection()
    print("\n--- Subject performance (lowest pass rate first) ---")
    print(pd.read_sql_query("SELECT * FROM analytics_subject_performance", conn).to_string(index=False))
    print("\n--- Hardest questions (top 5 by miss rate) ---")
    print(
        pd.read_sql_query(
            "SELECT subject_name, question_text, times_answered, miss_rate "
            "FROM analytics_question_difficulty ORDER BY miss_rate DESC LIMIT 5",
            conn,
        ).to_string(index=False)
    )
    conn.close()
