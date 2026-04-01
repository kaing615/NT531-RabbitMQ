#!/usr/bin/env bash
set -euo pipefail

# ===== Config (override bằng env nếu cần) =====
WORK_DIR="${WORK_DIR:-${HOME}/work-queue}"
QUEUE_NAME="${QUEUE_NAME:-orders_queue}"

RABBIT_HOST="${RABBIT_HOST:-}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

if [[ -z "${RABBIT_HOST}" ]]; then
  echo "ERROR: Missing RABBIT_HOST"
  exit 1
fi

SLEEP_MS="${SLEEP_MS:-20}"
PREFETCH="${PREFETCH:-1}"
QUEUE_DURABLE="${QUEUE_DURABLE:-1}"

# ===== Broker node (remote EC2) =====
BROKER_SSH_HOST="${BROKER_SSH_HOST:-}"
BROKER_SSH_USER="${BROKER_SSH_USER:-}"
BROKER_CONTAINER="${BROKER_CONTAINER:-}"

if [[ -z "${BROKER_SSH_HOST}" || -z "${BROKER_SSH_USER}" || -z "${BROKER_CONTAINER}" ]]; then
  echo "ERROR: Missing broker config."
  echo "Please set:"
  echo "  BROKER_SSH_HOST"
  echo "  BROKER_SSH_USER"
  echo "  BROKER_CONTAINER"
  exit 1
fi

CPU_DELAY="${CPU_DELAY:-3}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

# ===== Output =====
OUTPUT_DIR="${OUTPUT_DIR:-${WORK_DIR}/results/single-run}"
CPU_LOG="${CPU_LOG:-${OUTPUT_DIR}/cpu_rabbit.log}"
MEM_LOG="${MEM_LOG:-${OUTPUT_DIR}/mem_rabbit.log}"
PID_FILE="${PID_FILE:-${OUTPUT_DIR}/workers.pids}"
TS_CSV="${TS_CSV:-${OUTPUT_DIR}/queue_ts.csv}"

usage() {
  cat <<EOF
Usage:
  ./run_workers_until_drained.sh <NUM_WORKERS>

Stops automatically when:
  messages_ready == 0 AND messages_unacknowledged == 0
  for STABLE_ZERO_COUNT consecutive polls.

Env override (optional):
  WORK_DIR, OUTPUT_DIR, CPU_LOG, MEM_LOG, PID_FILE, TS_CSV
  QUEUE_NAME, SLEEP_MS, PREFETCH, QUEUE_DURABLE
  RABBIT_HOST, RABBIT_PORT, RABBIT_USER, RABBIT_PASS
  BROKER_SSH_HOST, BROKER_SSH_USER, BROKER_CONTAINER
  CPU_DELAY, POLL_INTERVAL, STABLE_ZERO_COUNT
EOF
}

NUM_WORKERS="${1:-}"
if [[ -z "${NUM_WORKERS}" || ! "${NUM_WORKERS}" =~ ^[0-9]+$ || "${NUM_WORKERS}" -lt 1 ]]; then
  usage
  exit 1
fi

mkdir -p "${WORK_DIR}"
mkdir -p "${OUTPUT_DIR}"

if [[ -f "${WORK_DIR}/.venv/bin/activate" ]]; then
  source "${WORK_DIR}/.venv/bin/activate"
fi

CPU_PID=""

remote_sh() {
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${BROKER_SSH_USER}@${BROKER_SSH_HOST}" "$@"
}

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
  python3 - "${CPU_LOG}" <<'PY'
import statistics as st, sys
vals=[]
try:
    for line in open(sys.argv[1], "r", encoding="utf-8"):
        parts = line.strip().split(",")
        if len(parts) >= 3:
            try:
                vals.append(float(parts[1]))
            except:
                pass
except FileNotFoundError:
    pass

if not vals:
    print("No CPU samples found.")
else:
    print("samples", len(vals),
          "avg_cpu%", round(st.mean(vals), 2),
          "max_cpu%", round(max(vals), 2))
PY

  echo "== MEM avg/max from ${MEM_LOG} =="
  python3 - "${MEM_LOG}" <<'PY'
import re, statistics as st, sys

def to_mib(s):
    s = s.strip()
    m = re.search(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)", s, re.I)
    if not m:
        return 0.0
    v = float(m.group(1))
    u = m.group(2).lower()
    if u == "b":
        return v / (1024 * 1024)
    if u == "kib":
        return v / 1024
    if u == "mib":
        return v
    if u == "gib":
        return v * 1024
    if u == "tib":
        return v * 1024 * 1024
    return 0.0

vals=[]
try:
    for line in open(sys.argv[1], "r", encoding="utf-8"):
        parts = line.strip().split(",")
        if len(parts) >= 3:
            vals.append(to_mib(parts[1]))
except FileNotFoundError:
    pass

if not vals:
    print("No MEM samples found.")
else:
    print("samples", len(vals),
          "avg_mem_MiB", round(st.mean(vals), 2),
          "max_mem_MiB", round(max(vals), 2))
PY

  echo "== Worker log lines (distribution) =="
  wc -l "${OUTPUT_DIR}"/w*.jsonl 2>/dev/null || true
}

trap cleanup EXIT

echo "== (0) Check SSH to broker ${BROKER_SSH_USER}@${BROKER_SSH_HOST} =="
remote_sh "hostname" >/dev/null

echo "== (1) Cleanup run logs in ${OUTPUT_DIR} =="
rm -f "${OUTPUT_DIR}"/w*.jsonl || true
rm -f "${CPU_LOG}" || true
rm -f "${MEM_LOG}" || true
rm -f "${PID_FILE}" || true
rm -f "${TS_CSV}" || true

echo "== (2) Start ${NUM_WORKERS} workers sleep=${SLEEP_MS}ms prefetch=${PREFETCH} =="
for i in $(seq 1 "${NUM_WORKERS}"); do
  LOG="${OUTPUT_DIR}/w${i}.jsonl"
  python3 "${WORK_DIR}/worker.py" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --queue "${QUEUE_NAME}" \
    --queue-durable "${QUEUE_DURABLE}" \
    --worker-id "${i}" \
    --sleep-ms "${SLEEP_MS}" \
    --prefetch "${PREFETCH}" \
    --log "${LOG}" \
    >/dev/null 2>&1 &
  echo $! >> "${PID_FILE}"
done
echo "PIDs saved to ${PID_FILE}"

echo "ts_iso,ts_epoch,ready,unacked,total" > "${TS_CSV}"

echo "== (3) Wait ${CPU_DELAY}s then start CPU/MEM sampling on remote broker =="
sleep "${CPU_DELAY}"

(
  while true; do
    ts_epoch="$(date +%s)"

    cpu="$(
      remote_sh "docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' ${BROKER_CONTAINER} 2>/dev/null | awk 'NF>=2 {gsub(/%/, \"\", \$2); print \$2}'" \
      2>/dev/null || true
    )"
    cpu="${cpu:-0}"

    mem="$(
      remote_sh "docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' ${BROKER_CONTAINER} 2>/dev/null | awk 'NF>=2 {print \$2}'" \
      2>/dev/null || true
    )"
    mem="${mem:-0MiB}"

    # Giữ format tương thích summarize_run.py: ts,val1,val2
    echo "${ts_epoch},${cpu},0" >> "${CPU_LOG}"
    echo "${ts_epoch},${mem},0MiB" >> "${MEM_LOG}"

    sleep 1
  done
) &
CPU_PID="$!"
echo "CPU logger PID: ${CPU_PID}"

get_ready_unacked() {
  remote_sh "docker exec ${BROKER_CONTAINER} rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null | awk '\$1==\"${QUEUE_NAME}\" {print \$2, \$3; found=1} END{if(!found) print \"0 0\"}'" \
    || remote_sh "rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null | awk '\$1==\"${QUEUE_NAME}\" {print \$2, \$3; found=1} END{if(!found) print \"0 0\"}'"
}

echo "== (4) Waiting until queue drained: ready=0 and unacked=0 (stable ${STABLE_ZERO_COUNT} polls) =="

zero_ok=0
while true; do
  read -r ready unacked < <(get_ready_unacked)
  ready="${ready:-0}"
  unacked="${unacked:-0}"

  ts_iso="$(TZ=Asia/Ho_Chi_Minh date '+%Y-%m-%d %H:%M:%S')"
  ts_epoch="$(date +%s)"
  total=$((ready + unacked))

  echo "[${ts_iso}] ready=${ready} unacked=${unacked}"
  echo "${ts_iso},${ts_epoch},${ready},${unacked},${total}" >> "${TS_CSV}"

  if [[ "${ready}" == "0" && "${unacked}" == "0" ]]; then
    zero_ok=$((zero_ok + 1))
  else
    zero_ok=0
  fi

  if [[ "${zero_ok}" -ge "${STABLE_ZERO_COUNT}" ]]; then
    echo "== Drained confirmed (ready=0 & unacked=0 stable ${STABLE_ZERO_COUNT} times). Exiting =="
    exit 0
  fi

  sleep "${POLL_INTERVAL}"
done