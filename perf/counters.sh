#!/usr/bin/env bash
# Hardware counters per ladder rung.
#
# The counter that decides everything here is context-switches: a rung doing
# real work has ~0 per op, a rung crossing threads has ~2 per op, and on a
# machine whose idle exit latency is measured in tens of microseconds that
# difference *is* the runtime. cycles/instructions are printed alongside so a
# memory- or CPU-bound explanation can be ruled out rather than assumed.
#
# usage: perf/counters.sh [--dir DIR] [seconds] [rung ...]
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-.venv/bin/python}
DIRARG=()
if [[ ${1:-} == --dir ]]; then DIRARG=(--dir "$2"); shift 2; fi
SECONDS_PER=${1:-3}; shift || true
RUNGS=("$@"); [[ ${#RUNGS[@]} -eq 0 ]] && RUNGS=(pread future bridge read try_read file_read read_bytes)

CTRS=task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions

if ! perf stat -e cycles -- true >/dev/null 2>&1; then
  echo "perf cannot open events. Grant it CAP_PERFMON:" >&2
  echo "  sudo setcap 'cap_perfmon,cap_bpf,cap_sys_ptrace,cap_dac_read_search+ep' \$(command -v perf)" >&2
  exit 1
fi

for rung in "${RUNGS[@]}"; do
  echo "=== $rung ==="
  perf stat -e "$CTRS" -- "$PY" perf/ladder.py --only "$rung" \
      --seconds "$SECONDS_PER" "${DIRARG[@]}" 2>&1 \
    | grep -E 'ops in|task-clock|context-switches|cpu-migrations|page-faults|cycles|instructions|insn per' \
    | sed 's/^ */  /'
done
