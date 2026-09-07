#!/usr/bin/env bash
# Bootstrap this repo on a fresh machine.
#
#   git clone <this repo> && cd DE && ./setup.sh
#
# Creates a venv, installs the Python dependencies, and runs the standalone
# test suite so you know the logic works here BEFORE bringing up Docker.
# Does not start any containers -- that's a separate, explicit step.

set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mok\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mwarn\033[0m  %s\n' "$*"; }
die()  { printf '  \033[31mfail\033[0m  %s\n' "$*"; exit 1; }

say "1. Checking prerequisites"

command -v python3 >/dev/null || die "python3 not found. Install Python 3.10+ and re-run."
PYV="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)' \
  || die "python3 is $PYV; this needs 3.10 or newer."
ok "python3 $PYV"

python3 -c 'import venv' 2>/dev/null \
  || die "python3-venv is missing. On Debian/Ubuntu: sudo apt install python3-venv"
ok "python3-venv present"

if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
  ok "docker $(docker --version | awk '{print $3}' | tr -d ,) (daemon reachable)"
  if docker compose version >/dev/null 2>&1; then
    ok "docker compose $(docker compose version --short 2>/dev/null || echo present)"
  else
    warn "'docker compose' not available -- you can still run the tests, but not Airflow."
  fi
else
  warn "docker not available or daemon not running."
  warn "The tests below will still work; Airflow needs Docker."
fi

say "2. Creating the virtualenv"

if [ -d .venv ]; then
  ok ".venv already exists, reusing it"
else
  python3 -m venv .venv
  ok "created .venv"
fi
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r gharchive-pipeline/requirements.txt
ok "installed: $(.venv/bin/python -c 'import pandas; print("pandas " + pandas.__version__)')"

say "3. Running the standalone test suite"
echo "  (first run downloads one ~73 MB hour from GH Archive; later runs reuse it)"
echo

cd "$ROOT/gharchive-pipeline/scripts"
if "$ROOT/.venv/bin/python" test_gharchive.py; then
  cd "$ROOT"
  say "Ready."
  cat <<'EOF'
The pipeline logic is verified on this machine. To bring up Airflow:

  cd gharchive-pipeline
  cp .env.example .env        # only if port 8080 or 5432 is taken here
  docker compose up -d
  docker compose ps           # wait for apiserver to report healthy

  # UI: http://localhost:8080   (admin / admin)

Run one hour end to end and watch it happen:

  docker compose exec airflow-scheduler \
    airflow dags test gharchive_hourly 2026-09-06T13:00:00+00:00

See what landed:

  docker compose exec analytics-db psql -U analytics -d analytics \
    -c "SELECT event_type, events FROM gh_event_type_hourly ORDER BY events DESC;"

Read HANDOFF.md for the full picture, including which external hosts are
reachable from which network -- that has bitten this project before.
EOF
else
  cd "$ROOT"
  say "Tests failed."
  cat <<'EOF'
If the failure is the missing-hour check, it needs network access to
data.gharchive.org -- confirm with:

  curl -sS -o /dev/null -w '%{http_code}\n' https://data.gharchive.org/2026-09-06-12.json.gz

A timeout there means the host is blocked on this network, not a bug in
the code. See HANDOFF.md.
EOF
  exit 1
fi
