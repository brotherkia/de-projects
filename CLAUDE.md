# Working in this repo

A data-engineering learning repo. Parsa is moving into DE from telecom
billing/operations and is learning the toolchain by building real pipelines.
See `HANDOFF.md` for background and `gharchive-pipeline/README.md` for the
current project.

## The rule that shapes everything here

**Nothing is described as working until it has actually been run.** This isn't
a style preference — the point of the repo is that resume claims stay true, so
"I wrote code for this" must never be blurred into "this works."

When reporting on work here, always separate:

- **Verified** — you ran it and saw the output
- **Written but not run** — code exists, no evidence it works

Both READMEs and `HANDOFF.md` have explicit "Verified" / "Not yet done"
sections. Keep them accurate; move items between them only on evidence.

## Measure, don't estimate

Several confident guesses made while building this were wrong by large
factors, and each was caught only by checking:

- per-hour event volume — guessed 1.3M, actually ~72k (18x off)
- distinct repos per hour — guessed ~500k, actually ~25k (20x off)
- a "missing" archive hour — actually a zero-padding bug in the URL
- a top-500 repo cap that silently discarded 75% of events

So: before writing a number into a comment, a docstring or a commit message,
run something that produces it. Numbers in this codebase are expected to be
measured, and several docstrings cite the exact hour they were measured on.

## Network reachability is a real constraint

This machine's network silently blocks some hosts at the TCP level: DNS
resolves, then the connection times out.

- **Blocked:** `books.toscrape.com`, `api.binance.com`
- **Working:** `data.gharchive.org`, `stream.wikimedia.org`,
  `dumps.wikimedia.org`, `api.open-meteo.com`, `opensky-network.org`,
  `api.coingecko.com`, `api.github.com`, `huggingface.co`, `archive.org`,
  NYC TLC on CloudFront, Docker Hub, `raw.githubusercontent.com`

**Before designing anything around an external data source, curl it first and
confirm real data comes back — not just an HTTP 200.** An entire subproject
(`book-scraper/`, deleted at `cb99250`) was built and thrown away because its
only data source was unreachable, and that surfaced only at the last step.

A `curl: (28)` timeout is a network block. Don't debug imports, paths or
directory layout in response to one.

## Layout convention

Logic lives in `scripts/`, with **zero Airflow imports**. The DAG in `dags/`
is a thin wrapper handling only scheduling, retries and skip-on-missing.

This is deliberate: it means the ETL logic is provable with
`python3 test_gharchive.py` and no containers, so a failure inside Airflow is
always a scheduling problem and never a "does the maths work" problem. Keep new
logic on the `scripts/` side of that line.

## Running things

```bash
./setup.sh                                    # fresh machine: venv + deps + 19 checks
cd gharchive-pipeline/scripts && python3 test_gharchive.py   # logic only, no Docker
cd gharchive-pipeline && docker compose up -d                # Airflow 3.3.1
```

Use `.venv/bin/python`, not the system `python3` — pandas is only in the venv.

Host ports come from `.env` (gitignored, see `.env.example`); compose defaults
to 8080/5432. On Parsa's main machine 8080 is taken by an unrelated
`cute-site` container, so `.env` there sets 8081/5433. Don't hardcode either.

`.env` also carries the Airflow secrets (`AIRFLOW__CORE__FERNET_KEY`,
`AIRFLOW__API_AUTH__JWT_SECRET`). Compose refuses to start without them, and
`./setup.sh` generates them on first run. Never commit a real value — this repo
is public, and its history was rewritten once already to remove a pair.

## Git

Remote is **hamgit.ir** (`kiaee7`), credentials already stored. GitHub is
reachable but the SSH key at `~/.ssh/id_ed25519.pub` is not registered on the
account, so GitHub pushes fail until it is.

Commit messages here explain *why*, and state what was verified versus what
was only written. Match that.
