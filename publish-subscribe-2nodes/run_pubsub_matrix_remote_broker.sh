#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-${HOME}/pubsub-baseline}"
RUN_ONE="${RUN_ONE:-${WORK_DIR}/run_one_remote_broker_pubsub.sh}"

RATES=(${RATES:-500 1000 2000})
PREFETCH_LIST=(${PREFETCH_LIST:-10 50 200})
SIZES=(${SIZES:-1024 10240})
MODES=(${MODES:-A B})
M="${M:-3}"
RUN_SECONDS="${RUN_SECONDS:-20}"

TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
mkdir -p "${OUT_ROOT}"

for mode in "${MODES[@]}"; do
  for rate in "${RATES[@]}"; do
    for prefetch in "${PREFETCH_LIST[@]}"; do
      for size in "${SIZES[@]}"; do
        echo
        echo "===== PUBSUB RUN mode=${mode} rate=${rate} M=${M} prefetch=${prefetch} size=${size} ====="

        WORK_DIR="${WORK_DIR}" \
        OUT_ROOT="${OUT_ROOT}" \
        MODE="${mode}" \
        RATE="${rate}" \
        M="${M}" \
        PREFETCH="${prefetch}" \
        SIZE="${size}" \
        RUN_SECONDS="${RUN_SECONDS}" \
        "${RUN_ONE}"
      done
    done
  done
done

echo
echo "DONE: ${OUT_ROOT}/summary.csv"