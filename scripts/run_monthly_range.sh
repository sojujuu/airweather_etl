#!/usr/bin/env zsh
# File: run_monthly_range.zsh
# Usage:
#   chmod +x run_monthly_range.zsh
#   ./run_monthly_range.zsh                         # defaults 2024-01-01..2024-12-31
#   ./run_monthly_range.zsh 2016-01-01 2016-12-31   # custom range
#   ./run_monthly_range.zsh --dry-run               # preview only

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

# --- Generate month-ends via Python (handles leap years & varying month lengths) ---
month_ends=("${(@f)$(/usr/bin/env python3 - <<'PY' "$START_DATE" "$END_DATE"
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

def month_end(dt: date) -> date:
    # first day of next month minus 1 day
    if dt.month == 12:
        first_next = date(dt.year + 1, 1, 1)
    else:
        first_next = date(dt.year, dt.month + 1, 1)
    return first_next - timedelta(days=1)

def add_month(dt: date) -> date:
    # move to first day of next month
    if dt.month == 12:
        return date(dt.year + 1, 1, 1)
    return date(dt.year, dt.month + 1, 1)

start = parse(start_s)
end = parse(end_s)
if end < start:
    print("ERROR: END_DATE must be on/after START_DATE.", file=sys.stderr)
    sys.exit(2)

# Start from the first day of start's month
cur = date(start.year, start.month, 1)

while cur <= end:
    me = month_end(cur)                     # actual last day of this month (handles Feb 29 in leap years)
    if me >= start and me <= end:
        print(me.isoformat())
    cur = add_month(cur)
PY
)}")

if (( ${#month_ends[@]} == 0 )); then
  echo "No month-ends found in range ${START_DATE}..${END_DATE}."
  exit 0
fi

echo "Found ${#month_ends[@]} month-end dates between ${START_DATE} and ${END_DATE}."
if (( DRY_RUN )); then
  printf '%s\n' "${month_ends[@]}"
  echo "(dry-run) No ETL executed."
  exit 0
fi

for dt in "${month_ends[@]}"; do
  echo ">>> Running monthly ETL for ${dt}"
  uv run python scripts/run_etl.py --mode monthly --today "${dt}"
done

echo "All monthly runs completed."
