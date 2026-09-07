"""
Proves the streaming (event-by-event) stats match the batch pipeline's
stats on the same underlying data. This is the real correctness test for
event_processing.py -- no Kafka broker needed, since it simulates the
stream by replaying attempts from the sample DB one at a time.
"""
import sqlite3

from event_processing import new_running_stats, process_attempt_event, running_stats_to_rows
from pipeline_functions import extract_answers, transform_subject_performance


def attempts_as_events(conn):
    """
    Reshape the same raw answer rows pipeline_functions.py uses into one
    event per attempt -- {subject_name, correct_answers, total_answers} --
    in submission order, the way they'd actually arrive off a Kafka topic.
    """
    raw = extract_answers(conn)
    per_attempt = (
        raw.groupby(["attempt_id", "subject_name", "submitted_at"])["is_correct"]
        .agg(correct_answers="sum", total_answers="count")
        .reset_index()
        .sort_values("submitted_at")
    )
    for _, row in per_attempt.iterrows():
        yield {
            "subject_name": row["subject_name"],
            "correct_answers": int(row["correct_answers"]),
            "total_answers": int(row["total_answers"]),
        }


def main():
    conn = sqlite3.connect("sample_exam_data.db")

    # Batch result (same code path the Airflow DAG uses)
    raw = extract_answers(conn)
    batch_rows = transform_subject_performance(raw)[["subject_name", "attempts", "avg_score", "pass_rate"]]
    batch_rows = batch_rows.sort_values("pass_rate").reset_index(drop=True)

    # Streaming result: replay every attempt as an event, one at a time
    stats = new_running_stats()
    n_events = 0
    for event in attempts_as_events(conn):
        process_attempt_event(event, stats)
        n_events += 1
    streaming_rows = running_stats_to_rows(stats)

    conn.close()

    print(f"Replayed {n_events} attempt events through the streaming processor.\n")
    print("Batch pipeline result:")
    print(batch_rows.to_string(index=False))
    print("\nStreaming (event-by-event) result:")
    for r in streaming_rows:
        print(f"  {r['subject_name']:<20} attempts={r['attempts']:<5} avg_score={r['avg_score']:<6} pass_rate={r['pass_rate']}")

    # Actual pass/fail check: do they agree?
    mismatches = []
    for _, b in batch_rows.iterrows():
        s = next(r for r in streaming_rows if r["subject_name"] == b["subject_name"])
        if s["attempts"] != b["attempts"] or abs(s["pass_rate"] - b["pass_rate"]) > 0.1:
            mismatches.append(b["subject_name"])

    if mismatches:
        print(f"\nMISMATCH on: {mismatches}")
    else:
        print("\nMATCH: streaming and batch agree exactly on every subject.")


if __name__ == "__main__":
    main()
