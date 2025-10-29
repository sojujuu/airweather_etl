#!/usr/bin/env zsh
# File: run_weekly_range.zsh
# Usage:
#   chmod +x run_weekly_range.zsh
#   ./run_weekly_range.zsh                         # defaults 2024-01-01..2024-12-31
#   ./run_weekly_range.zsh 2024-01-01 2024-12-31
#   ./run_weekly_range.zsh --dry-run               # preview only
#   ./run_weekly_range.zsh -n 2014-01-01 2014-12-31

set -euo pipefail

# --- Parse flags (only --dry-run / -n) ---
DRY_RUN=0
typeset -a args
for a in "$@"; do
  if [[ "$a" == "--dry-run" || "$a" == "-n" ]]; then
    DRY_RUN=1
  else
    args+=("$a")
  fi
done

START_DATE=${args[1]:-2014-01-01}
END_DATE=${args[2]:-2024-12-31}

# Move to project root (assumes this file lives in ./scripts/)
SCRIPT_DIR=${0:A:h}
PROJECT_ROOT="${SCRIPT_DIR:h}"
cd "$PROJECT_ROOT"

# --- Generate all Sundays via Python (handles leap years) ---
sundays=("${(@f)$(/usr/bin/env python3 - <<'PY' "$START_DATE" "$END_DATE"
import sys
from datetime import date, timedelta

start_s, end_s = sys.argv[1], sys.argv[2]

def parse(d):
    try:
        y, m, dd = map(int, d.split("-"))
        return date(y, m, dd)
    except Exception:
        print("ERROR: invalid date format. Use YYYY-MM-DD.", file=sys.stderr)
        sys.exit(2)

start = parse(start_s)
end = parse(end_s)
if end < start:
    print("ERROR: END_DATE must be on/after START_DATE.", file=sys.stderr)
    sys.exit(2)

# Monday=0 ... Sunday=6
days_to_sunday = (6 - start.weekday()) % 7
first_sunday = start + timedelta(days=days_to_sunday)

d = first_sunday
while d <= end:
    print(d.isoformat())
    d += timedelta(days=7)
PY
)}")

if (( ${#sundays[@]} == 0 )); then
  echo "No Sundays found in range ${START_DATE}..${END_DATE}."
  exit 0
fi

echo "Found ${#sundays[@]} Sundays between ${START_DATE} and ${END_DATE}."
if (( DRY_RUN )); then
  printf '%s\n' "${sundays[@]}"
  echo "(dry-run) No ETL executed."
  exit 0
fi

for dt in "${sundays[@]}"; do
  echo ">>> Running weekly ETL for ${dt}"
  uv run python scripts/run_etl.py --mode weekly --today "${dt}"
done

echo "All weekly runs completed."
