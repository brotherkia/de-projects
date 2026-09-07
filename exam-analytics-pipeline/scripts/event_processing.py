"""
Pure incremental event-processing logic for the streaming (Kafka) layer.

Deliberately has zero Kafka dependency -- it just takes one event dict at a
time and updates a running-stats dict. That's what makes it testable without
a broker: kafka_consumer.py is a thin wrapper that calls process_attempt_event
per message; test_event_processing.py exercises this file directly.

This mirrors pipeline_functions.transform_subject_performance() exactly
(same "pass" definition: per-attempt score >= 60%), just computed one event
at a time instead of via a batch groupby -- so the two should agree on the
same underlying data. That agreement is the actual correctness test.
"""


def new_running_stats() -> dict:
    """Empty running-stats accumulator: subject_name -> counters."""
    return {}


def process_attempt_event(event: dict, running_stats: dict, pass_threshold: float = 0.6) -> dict:
    """
    event must have: subject_name, correct_answers, total_answers.
    Updates running_stats in place (and returns it, for convenient chaining).
    """
    subject = event["subject_name"]
    total = event["total_answers"]
    if total == 0:
        return running_stats  # malformed event; nothing to score

    attempt_score = event["correct_answers"] / total
    passed = attempt_score >= pass_threshold

    bucket = running_stats.setdefault(subject, {"attempts": 0, "score_sum": 0.0, "passed_count": 0})
    bucket["attempts"] += 1
    bucket["score_sum"] += attempt_score
    bucket["passed_count"] += int(passed)
    return running_stats


def running_stats_to_rows(running_stats: dict) -> list:
    """Same shape as pipeline_functions.transform_subject_performance()'s output."""
    rows = []
    for subject, b in running_stats.items():
        rows.append(
            {
                "subject_name": subject,
                "attempts": b["attempts"],
                "avg_score": round(100 * b["score_sum"] / b["attempts"], 1),
                "pass_rate": round(100 * b["passed_count"] / b["attempts"], 1),
            }
        )
    return sorted(rows, key=lambda r: r["pass_rate"])
