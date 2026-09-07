"""
Creates a local SQLite database with a schema shaped like the real
Django app (question_bank.Subject/Question, attempts.ExamAttempt/Answer)
and fills it with plausible synthetic data, so the pipeline can be built
and tested end-to-end without needing the production database.

Swap this out once the pipeline runs against the real Postgres DB —
see pipeline_functions.py's get_connection().
"""
import random
import sqlite3
from datetime import datetime, timedelta

DB_PATH = "sample_exam_data.db"

SUBJECTS = ["Biology", "Chemistry", "Physics", "Logical Reasoning", "General Knowledge"]
N_QUESTIONS_PER_SUBJECT = 15
N_ATTEMPTS = 800

# Some questions are deliberately made "hard" (low true correctness probability)
# so the difficulty analysis has something real to surface.
HARD_QUESTION_RATE = 0.2


def build_schema(cur):
    cur.executescript(
        """
        DROP TABLE IF EXISTS answers;
        DROP TABLE IF EXISTS exam_attempts;
        DROP TABLE IF EXISTS questions;
        DROP TABLE IF EXISTS subjects;

        CREATE TABLE subjects (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE questions (
            id INTEGER PRIMARY KEY,
            subject_id INTEGER NOT NULL REFERENCES subjects(id),
            text TEXT NOT NULL,
            true_difficulty REAL NOT NULL  -- P(a random student answers correctly); hidden ground truth for validation
        );

        CREATE TABLE exam_attempts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            submitted_at TEXT NOT NULL
        );

        CREATE TABLE answers (
            id INTEGER PRIMARY KEY,
            attempt_id INTEGER NOT NULL REFERENCES exam_attempts(id),
            question_id INTEGER NOT NULL REFERENCES questions(id),
            is_correct INTEGER NOT NULL  -- 0/1
        );
        """
    )


def populate(cur):
    subject_ids = []
    for name in SUBJECTS:
        cur.execute("INSERT INTO subjects (name) VALUES (?)", (name,))
        subject_ids.append(cur.lastrowid)

    question_ids_by_subject = {sid: [] for sid in subject_ids}
    for sid in subject_ids:
        for i in range(N_QUESTIONS_PER_SUBJECT):
            is_hard = random.random() < HARD_QUESTION_RATE
            true_difficulty = random.uniform(0.15, 0.35) if is_hard else random.uniform(0.55, 0.92)
            cur.execute(
                "INSERT INTO questions (subject_id, text, true_difficulty) VALUES (?, ?, ?)",
                (sid, f"Sample question {i + 1} for subject {sid}", true_difficulty),
            )
            question_ids_by_subject[sid].append(cur.lastrowid)

    base_time = datetime(2026, 6, 1)
    for a in range(N_ATTEMPTS):
        user_id = random.randint(1, 250)
        started = base_time + timedelta(days=random.randint(0, 89), minutes=random.randint(0, 1000))
        submitted = started + timedelta(minutes=random.randint(20, 90))
        cur.execute(
            "INSERT INTO exam_attempts (user_id, started_at, submitted_at) VALUES (?, ?, ?)",
            (user_id, started.isoformat(), submitted.isoformat()),
        )
        attempt_id = cur.lastrowid

        # each attempt answers all questions from one randomly chosen subject
        sid = random.choice(subject_ids)
        for qid in question_ids_by_subject[sid]:
            cur.execute("SELECT true_difficulty FROM questions WHERE id = ?", (qid,))
            p_correct = cur.fetchone()[0]
            is_correct = 1 if random.random() < p_correct else 0
            cur.execute(
                "INSERT INTO answers (attempt_id, question_id, is_correct) VALUES (?, ?, ?)",
                (attempt_id, qid, is_correct),
            )


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    build_schema(cur)
    populate(cur)
    conn.commit()
    n_answers = cur.execute("SELECT COUNT(*) FROM answers").fetchone()[0]
    print(f"Created {DB_PATH} with {N_ATTEMPTS} attempts and {n_answers} answers.")
    conn.close()
