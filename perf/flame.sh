#!/usr/bin/env bash
# Sampled profile of one ladder rung, with kernel frames.
#
# Answers "where", after the ladder has answered "which layer". Emits a folded
# stack file that feeds FlameGraph directly if you have it, and a plain report
# either way.
#
# usage: perf/flame.sh [--dir DIR] <rung> [seconds]
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-.venv/bin/python}
OUT=${OUT:-target/perf}
DIRARG=()
if [[ ${1:-} == --dir ]]; then DIRARG=(--dir "$2"); shift 2; fi
RUNG=${1:?usage: perf/flame.sh [--dir DIR] <rung> [seconds]}
SECS=${2:-5}
mkdir -p "$OUT"

perf record -F 2999 -g --call-graph dwarf -o "$OUT/$RUNG.data" -- \
  "$PY" perf/ladder.py --only "$RUNG" --seconds "$SECS" "${DIRARG[@]}"

perf script -i "$OUT/$RUNG.data" > "$OUT/$RUNG.script"
if command -v stackcollapse-perf.pl >/dev/null && command -v flamegraph.pl >/dev/null; then
  stackcollapse-perf.pl "$OUT/$RUNG.script" > "$OUT/$RUNG.folded"
  flamegraph.pl "$OUT/$RUNG.folded" > "$OUT/$RUNG.svg"
  echo "flamegraph: $OUT/$RUNG.svg"
else
  echo "(FlameGraph not on PATH; raw samples in $OUT/$RUNG.script)"
  echo "  git clone https://github.com/brendangregg/FlameGraph"
fi

perf report -i "$OUT/$RUNG.data" --stdio --no-children -g none 2>/dev/null | head -30
