"""
Proves the DAG's XCom hand-off survives serialization.

The Airflow tasks don't pass DataFrames to each other directly -- each one
pushes `df.to_json()` into XCom and the next rebuilds it. That round trip is
real code that the other tests never touch (they call the transforms on an
in-memory DataFrame), which is exactly how a pandas-3 incompatibility in
df_from_json() sat here unnoticed: `pd.read_json(<string>)` used to work and
now reads the string as a file path.

So: run both transforms twice on the same data -- once straight through, once
with a to_json/df_from_json hop before and after, mirroring what the DAG
actually does -- and require identical results. No Airflow needed.
"""
import sqlite3

from pipeline_functions import (
    df_from_json,
    extract_answers,
    transform_question_difficulty,
    transform_subject_performance,
)


def main():
    conn = sqlite3.connect("sample_exam_data.db")
    raw = extract_answers(conn)
    conn.close()

    checks = []
    for name, transform in [
        ("subject_performance", transform_subject_performance),
        ("question_difficulty", transform_question_difficulty),
    ]:
        direct = transform(raw)

        # the DAG's actual path: extract -> XCom -> transform -> XCom -> load
        via_xcom = transform(df_from_json(raw.to_json()))
        via_xcom = df_from_json(via_xcom.to_json())

        # to_json/read_json doesn't preserve row order, and the load step
        # doesn't depend on it -- compare on content, sorted the same way.
        key = "subject_id" if name == "subject_performance" else "question_id"
        a = direct.sort_values(key).reset_index(drop=True)
        b = via_xcom.sort_values(key).reset_index(drop=True)
        b = b[a.columns]  # column order likewise isn't meaningful here

        same = a.equals(b)
        checks.append((name, same, len(a)))
        print(f"{name:<22} rows={len(a):<5} round-trip identical: {same}")

    if all(ok for _, ok, _ in checks):
        print("\nMATCH: the DAG's XCom round trip preserves both transforms exactly.")
    else:
        broken = [n for n, ok, _ in checks if not ok]
        print(f"\nMISMATCH on: {broken}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
