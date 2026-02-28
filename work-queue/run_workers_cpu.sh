#!/usr/bin/env bash
set -euo pipefail

# ===== Config (có thể override bằng env) =====
WORK_DIR="${HOME}/work-queue"
QUEUE_NAME="${QUEUE_NAME:-orders_queue}"

RABBIT_HOST="${RABBIT_HOST:-127.0.0.1}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

SLEEP_MS="${SLEEP_MS:-20}"
PREFETCH="${PREFETCH:-1}"

CONTAINER_NAME="${CONTAINER_NAME:-mq-lab-rabbitmq-1}"
CPU_LOG="cpu_rabbit.log"
CPU_DELAY="${CPU_DELAY:-10}"      # seconds before starting cpu log
CPU_SAMPLES="${CPU_SAMPLES:-60}"  # seconds of sampling (1 sample/sec)

PID_FILE="${WORK_DIR}/workers.pids"

usage() {
  cat <<EOF
Usage:
  ./run_workers_cpu.sh <NUM_WORKERS>

Env override (optional):
  QUEUE_NAME, RABBIT_HOST, RABBIT_PORT, RABBIT_USER, RABBIT_PASS
  SLEEP_MS (default ${SLEEP_MS}), PREFETCH (default ${PREFETCH})
  CONTAINER_NAME (default ${CONTAINER_NAME})
  CPU_DELAY (default ${CPU_DELAY}), CPU_SAMPLES (default ${CPU_SAMPLES})

Example:
  ./run_workers_cpu.sh 10
EOF
}

NUM_WORKERS="${1:-}"
if [[ -z "${NUM_WORKERS}" || ! "${NUM_WORKERS}" =~ ^[0-9]+$ || "${NUM_WORKERS}" -lt 1 ]]; then
  usage
  exit 1
fi

mkdir -p "${WORK_DIR}"

# Activate venv if exists
if [[ -f "${WORK_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "${WORK_DIR}/.venv/bin/activate"
fi

cleanup() {
  echo
  echo "== Stopping workers =="
  if [[ -f "${PID_FILE}" ]]; then
    while read -r pid; do
      [[ -z "${pid}" ]] && continue
      kill "${pid}" 2>/dev/null || true
    done < "${PID_FILE}"
    rm -f "${PID_FILE}" || true
  fi
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

echo "== (3) Wait ${CPU_DELAY}s then log CPU for ${CPU_SAMPLES}s =="
sleep "${CPU_DELAY}"

for ((i=1; i<=CPU_SAMPLES; i++)); do
  docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" "${CONTAINER_NAME}" >> "${CPU_LOG}"
  sleep 1
done

echo "== (4) CPU avg/max from ${CPU_LOG} =="
python3 - <<'PY'
import re, statistics as st
vals=[]
with open("cpu_rabbit.log","r",encoding="utf-8") as f:
    for line in f:
        m=re.search(r"([\d.]+)%", line)
        if m:
            vals.append(float(m.group(1)))
if not vals:
    print("No CPU samples found. Check CONTAINER_NAME / docker stats output.")
else:
    print("samples", len(vals),
          "avg_cpu%", round(st.mean(vals),2),
          "max_cpu%", round(max(vals),2))
PY

echo "== Worker log lines (distribution) =="
wc -l "${WORK_DIR}"/w*.jsonl || true

echo "== Done. Workers are still running. Press Ctrl+C to stop them =="
# giữ script sống để worker tiếp tục chạy cho tới khi bạn Ctrl+C
while true; do sleep 3600; done
