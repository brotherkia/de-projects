"""
Orchestrates the exam-analytics ETL: extract answers -> compute subject
pass rates + question difficulty -> load into analytics tables.

Runs nightly. Each step is its own task so a failure in one (e.g. the
difficulty transform) doesn't hide what succeeded, and so Airflow's UI
shows exactly where a run broke.
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# scripts/ is mounted into the Airflow containers alongside dags/ -- see docker-compose.yml
sys.path.append("/opt/airflow/scripts")

from pipeline_functions import (  # noqa: E402
    df_from_json,
    extract_answers,
    get_connection,
    load_dataframe,
    transform_question_difficulty,
    transform_subject_performance,
)

default_args = {
    "owner": "parsa",
    "retries": 2,
    "retry_delay": 300,  # seconds
}


def _extract(**context):
    conn = get_connection()
    try:
        df = extract_answers(conn)
    finally:
        conn.close()
    # XCom is fine for this data volume; for a genuinely large pull this
    # step would instead write to a staging table/file and pass a
    # path/reference through XCom rather than the data itself.
    context["ti"].xcom_push(key="raw_answers", value=df.to_json())


def _transform_subject_performance(**context):
    df = df_from_json(context["ti"].xcom_pull(key="raw_answers", task_ids="extract_answers"))
    result = transform_subject_performance(df)
    context["ti"].xcom_push(key="subject_performance", value=result.to_json())


def _transform_question_difficulty(**context):
    df = df_from_json(context["ti"].xcom_pull(key="raw_answers", task_ids="extract_answers"))
    result = transform_question_difficulty(df)
    context["ti"].xcom_push(key="question_difficulty", value=result.to_json())


def _load(**context):
    ti = context["ti"]
    subject_perf = df_from_json(ti.xcom_pull(key="subject_performance", task_ids="transform_subject_performance"))
    question_diff = df_from_json(ti.xcom_pull(key="question_difficulty", task_ids="transform_question_difficulty"))

    conn = get_connection()
    try:
        load_dataframe(conn, subject_perf, "analytics_subject_performance")
        load_dataframe(conn, question_diff, "analytics_question_difficulty")
    finally:
        conn.close()


with DAG(
    dag_id="exam_analytics_pipeline",
    description="Nightly subject pass-rate and question-difficulty analytics for the exam platform",
    default_args=default_args,
    schedule="0 2 * * *",  # 2am daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analytics", "exam-platform"],
) as dag:

    extract_task = PythonOperator(task_id="extract_answers", python_callable=_extract)

    transform_subject_task = PythonOperator(
        task_id="transform_subject_performance", python_callable=_transform_subject_performance
    )

    transform_difficulty_task = PythonOperator(
        task_id="transform_question_difficulty", python_callable=_transform_question_difficulty
    )

    load_task = PythonOperator(task_id="load_analytics_tables", python_callable=_load)

    extract_task >> [transform_subject_task, transform_difficulty_task] >> load_task
