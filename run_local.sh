#!/usr/bin/env bash

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INPUT="Retail_Transactions_Dataset.csv"
OUTPUT="output.txt"
ERROR_LOG="mapreduce_errors.log"

usage() {
  cat <<'EOF'
Usage: bash run_local.sh [options]

Runs a local MapReduce simulation:
  mapper.py | sort | reducer.py

Options:
  -i, --input FILE       Input CSV file (default: Retail_Transactions_Dataset.csv)
  -o, --output FILE      Output TSV file (default: output.txt)
  -e, --error-log FILE   Mapper/reducer stderr log (default: mapreduce_errors.log)
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--input)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      INPUT="$2"; shift 2 ;;
    -o|--output)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      OUTPUT="$2"; shift 2 ;;
    -e|--error-log)
      [[ $# -lt 2 ]] && { echo "Missing value for $1" >&2; usage; exit 1; }
      ERROR_LOG="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

PY=""
for cmd in python3 python; do
  if command -v "$cmd" &>/dev/null && "$cmd" --version &>/dev/null 2>&1; then
    PY="$cmd"; break
  fi
done
[[ -z "$PY" ]] && { echo -e "${RED}ERROR: Python not found.${RESET}" >&2; exit 1; }

for f in "$INPUT" mapper.py reducer.py; do
  [[ -f "$f" ]] || { echo -e "${RED}ERROR: Not found: $f${RESET}" >&2; exit 1; }
done

INPUT_ROWS=$(awk 'END{print NR-1}' "$INPUT")

echo -e "\n${BOLD}${CYAN}┌──────────────────────────────────────────────────┐"
echo -e "│   MapReduce Local Test Runner                    │"
echo -e "│   Dataset: Retail Transactions (1M rows)         │"
echo -e "└──────────────────────────────────────────────────┘${RESET}"
echo -e "  ${DIM}Python  →${RESET} $($PY --version 2>&1)"
echo -e "  ${DIM}Input   →${RESET} $INPUT ($INPUT_ROWS rows)"
echo ""
echo -e "${BOLD}Running pipeline...${RESET}"
echo -e "  ${DIM}[1/3]${RESET} Mapper   — emit (city, total_cost) per row"
echo -e "  ${DIM}[2/3]${RESET} Sort     — group by city (simulates Hadoop shuffle)"
echo -e "  ${DIM}[3/3]${RESET} Reducer  — sum revenue, count transactions per city"
echo ""

T0=$(date +%s%3N)
$PY mapper.py < "$INPUT" 2>"$ERROR_LOG" | sort | $PY reducer.py 2>>"$ERROR_LOG" > "$OUTPUT"
T1=$(date +%s%3N)
ELAPSED=$(( T1 - T0 ))

CITY_COUNT=$(awk 'NR>1{count++} END{print count+0}' "$OUTPUT")
TOTAL_TXN=$(awk -F'\t' 'NR>1{sum+=$3} END{print sum+0}' "$OUTPUT")
SKIPPED=$(wc -l < "$ERROR_LOG" | tr -d ' ')
TOTAL_REV=$(awk -F'\t' 'NR>1{sum+=$2} END{printf "%.2f", sum}' "$OUTPUT")

echo -e "${BOLD}Results (top 10 by revenue):${RESET}\n"
printf "  ${BOLD}${CYAN}%-25s %18s %15s${RESET}\n" "CITY" "TOTAL REVENUE (USD)" "TRANSACTIONS"
echo -e "  ${DIM}$(printf '─%.0s' {1..60})${RESET}"

tail -n +2 "$OUTPUT" | sort -t$'\t' -k2 -rn | head -10 | \
  awk -F'\t' 'NR==1{printf "  \033[0;32m%-25s %18s %15s\033[0m\n", $1, "$"$2, $3; next}
                    {printf "  %-25s %18s %15s\n", $1, "$"$2, $3}'

echo -e "  ${DIM}$(printf '─%.0s' {1..60})${RESET}"
printf "  ${BOLD}%-25s %18s %15s${RESET}\n" "TOTAL" "\$$TOTAL_REV" "$TOTAL_TXN"

echo ""
echo -e "${BOLD}${CYAN}Summary${RESET}"
echo -e "  ${GREEN}✓${RESET}  Cities found      : ${BOLD}$CITY_COUNT${RESET}"
echo -e "  ${GREEN}✓${RESET}  Input rows        : ${BOLD}$INPUT_ROWS${RESET}"
echo -e "  ${GREEN}✓${RESET}  Valid rows        : ${BOLD}$TOTAL_TXN${RESET}"
[[ "$SKIPPED" -gt 0 ]] && \
  echo -e "  ${YELLOW}⚠${RESET}   Rows skipped     : ${BOLD}$SKIPPED${RESET} ← see $ERROR_LOG" || \
  echo -e "  ${GREEN}✓${RESET}  Rows skipped      : ${BOLD}0${RESET}"
echo -e "  ${GREEN}✓${RESET}  Elapsed time      : ${BOLD}${ELAPSED} ms${RESET}"
echo -e "  ${GREEN}✓${RESET}  Output saved      : ${BOLD}$OUTPUT${RESET}"
echo ""