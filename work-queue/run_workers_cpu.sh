#!/usr/bin/env bash
set -euo pipefail

# ===== Config (override bằng env nếu cần) =====
WORK_DIR="${HOME}/work-queue"
QUEUE_NAME="${QUEUE_NAME:-orders_queue}"

RABBIT_HOST="${RABBIT_HOST:-127.0.0.1}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

SLEEP_MS="${SLEEP_MS:-20}"
PREFETCH="${PREFETCH:-1}"

CONTAINER_NAME="${CONTAINER_NAME:-mq-lab-rabbitmq-1}"
CPU_LOG="${CPU_LOG:-cpu_rabbit.log}"
CPU_DELAY="${CPU_DELAY:-10}"          # seconds before starting cpu log
POLL_INTERVAL="${POLL_INTERVAL:-2}"   # seconds
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"  # cần 0 ổn định N lần liên tiếp

PID_FILE="${WORK_DIR}/workers.pids"

usage() {
  cat <<EOF
Usage:
  ./run_workers_until_drained.sh <NUM_WORKERS>

Stops automatically when:
  messages_ready == 0 AND messages_unacked == 0
  for STABLE_ZERO_COUNT consecutive polls.

Env override (optional):
  QUEUE_NAME, SLEEP_MS, PREFETCH
  CONTAINER_NAME, CPU_DELAY, POLL_INTERVAL, STABLE_ZERO_COUNT
EOF
}

NUM_WORKERS="${1:-}"
if [[ -z "${NUM_WORKERS}" || ! "${NUM_WORKERS}" =~ ^[0-9]+$ || "${NUM_WORKERS}" -lt 1 ]]; then
  usage; exit 1
fi

mkdir -p "${WORK_DIR}"

# Activate venv if exists
if [[ -f "${WORK_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "${WORK_DIR}/.venv/bin/activate"
fi

CPU_PID=""
cleanup() {
  echo
  echo "== Stopping CPU logger =="
  [[ -n "${CPU_PID}" ]] && kill "${CPU_PID}" 2>/dev/null || true

  echo "== Stopping workers =="
  if [[ -f "${PID_FILE}" ]]; then
    while read -r pid; do
      [[ -z "${pid}" ]] && continue
      kill "${pid}" 2>/dev/null || true
    done < "${PID_FILE}"
    rm -f "${PID_FILE}" || true
  fi

  echo "== CPU avg/max from ${CPU_LOG} =="
  python3 - <<'PY'
import re, statistics as st
vals=[]
try:
    for line in open("cpu_rabbit.log","r",encoding="utf-8"):
        m=re.search(r"([\d.]+)%", line)
        if m: vals.append(float(m.group(1)))
except FileNotFoundError:
    vals=[]
if not vals:
    print("No CPU samples found.")
else:
    print("samples", len(vals),
          "avg_cpu%", round(st.mean(vals),2),
          "max_cpu%", round(max(vals),2))
PY

  echo "== Worker log lines (distribution) =="
  wc -l "${WORK_DIR}"/w*.jsonl 2>/dev/null || true
}
trap cleanup EXIT

echo "== (1) Cleanup logs =="
rm -f "${WORK_DIR}"/w*.jsonl || true
rm -f "${CPU_LOG}" || true
rm -f "${PID_FILE}" || true

echo "== (2) Start ${NUM_WORKERS} workers (1..${NUM_WORKERS}) sleep=${SLEEP_MS}ms prefetch=${PREFETCH} =="
for i in $(seq 1 "${NUM_WORKERS}"); do
  LOG="${WORK_DIR}/w${i}.jsonl"
  python3 "${WORK_DIR}/worker.py" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --queue "${QUEUE_NAME}" \
    --worker-id "${i}" \
    --sleep-ms "${SLEEP_MS}" \
    --prefetch "${PREFETCH}" \
    --log "${LOG}" \
    >/dev/null 2>&1 &
  echo $! >> "${PID_FILE}"
done
echo "PIDs saved to ${PID_FILE}"

echo "== (3) Wait ${CPU_DELAY}s then start CPU sampling (until drained) =="
sleep "${CPU_DELAY}"

# CPU logger chạy nền cho tới khi drained
(
  while true; do
    docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" "${CONTAINER_NAME}" >> "${CPU_LOG}" || true
    sleep 1
  done
) &
CPU_PID="$!"
echo "CPU logger PID: ${CPU_PID}"

# Hàm đọc ready/unacked từ rabbitmqctl trong container
get_ready_unacked() {
  # Output dạng: "<ready> <unacked>"
  docker exec "${CONTAINER_NAME}" rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null \
    | awk -v q="${QUEUE_NAME}" '$1==q {print $2, $3; found=1} END{if(!found) print "0 0"}'
}

echo "== (4) Waiting until queue drained: ready=0 and unacked=0 (stable ${STABLE_ZERO_COUNT} polls) =="
zero_ok=0
while true; do
  read -r ready unacked < <(get_ready_unacked)
  ready="${ready:-0}"
  unacked="${unacked:-0}"

  ts="$(date '+%H:%M:%S')"
  echo "[${ts}] ready=${ready} unacked=${unacked}"

  if [[ "${ready}" == "0" && "${unacked}" == "0" ]]; then
    zero_ok=$((zero_ok+1))
  else
    zero_ok=0
  fi

  if [[ "${zero_ok}" -ge "${STABLE_ZERO_COUNT}" ]]; then
    echo "== Drained confirmed (ready=0 & unacked=0 stable ${STABLE_ZERO_COUNT} times). Exiting =="
    exit 0
  fi

  sleep "${POLL_INTERVAL}"
done