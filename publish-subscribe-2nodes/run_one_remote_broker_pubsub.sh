#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-${HOME}/pubsub-baseline}"

RABBIT_HOST="${RABBIT_HOST:?set to node2 broker IP/DNS}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

BROKER_SSH="${BROKER_SSH:?set like azureuser@10.0.0.5}"
BROKER_RMQCTL="${BROKER_RMQCTL:-sudo rabbitmqctl}"

PRODUCER_PY="${PRODUCER_PY:-${WORK_DIR}/producer_pubsub.py}"
SUB_RUNNER="${SUB_RUNNER:-${WORK_DIR}/run_subscribers_until_drained_remote_broker_pubsub.sh}"
SUMMARIZER="${SUMMARIZER:-${WORK_DIR}/summarize_pubsub_run.py}"

MODE="${MODE:-A}"
RATE="${RATE:-500}"
M="${M:-3}"
PREFETCH="${PREFETCH:-10}"
SIZE="${SIZE:-1024}"
RUN_SECONDS="${RUN_SECONDS:-30}"
SLEEP_MS="${SLEEP_MS:-20}"
EXCHANGE_BASE="${EXCHANGE_BASE:-events_fanout}"
QUEUE_BASE_PREFIX="${QUEUE_BASE_PREFIX:-events_sub_}"
POLL_INTERVAL="${POLL_INTERVAL:-3}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

usage() {
  cat <<EOF
Usage:
  ./run_one_remote_broker_pubsub.sh [--mode A|B] [--rate 500] [--m 3] [--prefetch 10] [--size 1024] [--seconds 30]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --m|--subs|--subscribers) M="$2"; shift 2 ;;
    --prefetch) PREFETCH="$2"; shift 2 ;;
    --size|--payload-bytes) SIZE="$2"; shift 2 ;;
    --seconds) RUN_SECONDS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

broker_exec() {
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${BROKER_SSH}" "$@"
}

purge_queue_remote() {
  local q="$1"
  broker_exec "${BROKER_RMQCTL} purge_queue '${q}' >/dev/null 2>&1 || true"
}

DURABLE="0"
PRODUCER_FLAGS=""
EXCHANGE_NAME=""
QUEUE_BASE=""

case "${MODE}" in
  A)
    DURABLE="0"
    PRODUCER_FLAGS=""
    EXCHANGE_NAME="${EXCHANGE_BASE}_A"
    QUEUE_BASE="${QUEUE_BASE_PREFIX}A_"
    ;;
  B)
    DURABLE="1"
    PRODUCER_FLAGS="--durable --persistent"
    EXCHANGE_NAME="${EXCHANGE_BASE}_B"
    QUEUE_BASE="${QUEUE_BASE_PREFIX}B_"
    ;;
  *)
    echo "Invalid MODE=${MODE} (use A or B)"
    exit 1
    ;;
esac

TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
messages=$((RATE * RUN_SECONDS))
run_tag="mode${MODE}_rate${RATE}_M${M}_pref${PREFETCH}_sz${SIZE}_t${RUN_SECONDS}"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
RUN_DIR="${OUT_ROOT}/${run_tag}"
mkdir -p "${RUN_DIR}"

CPU_LOG="cpu_rabbit.log"
MEM_LOG="mem_rabbit.log"
QUEUE_TS="queue_ts.csv"
PRODUCER_DONE_FILE="${RUN_DIR}/producer.done"

echo "=== ONE PUBSUB RUN (REMOTE BROKER) ==="
echo "run_tag:       ${run_tag}"
echo "broker host:   ${RABBIT_HOST}"
echo "broker ssh:    ${BROKER_SSH}"
echo "mode:          ${MODE}"
echo "exchange:      ${EXCHANGE_NAME}"
echo "subscribers M: ${M}"
echo "rate:          ${RATE} msg/s"
echo "prefetch:      ${PREFETCH}"
echo "payload bytes: ${SIZE}"
echo "duration:      ${RUN_SECONDS}s -> messages=${messages}"
echo "out:           ${RUN_DIR}"
echo

rm -f "${PRODUCER_DONE_FILE}"

for i in $(seq 1 "${M}"); do
  q="${QUEUE_BASE}${i}"
  purge_queue_remote "${q}"
done

OUTPUT_DIR="${RUN_DIR}" \
CPU_LOG="${CPU_LOG}" \
MEM_LOG="${MEM_LOG}" \
QUEUE_TS="${QUEUE_TS}" \
PRODUCER_DONE_FILE="${PRODUCER_DONE_FILE}" \
EXCHANGE_NAME="${EXCHANGE_NAME}" \
QUEUE_BASE="${QUEUE_BASE}" \
DURABLE="${DURABLE}" \
PREFETCH="${PREFETCH}" \
SLEEP_MS="${SLEEP_MS}" \
BROKER_SSH="${BROKER_SSH}" \
BROKER_RMQCTL="${BROKER_RMQCTL}" \
RABBIT_HOST="${RABBIT_HOST}" \
RABBIT_PORT="${RABBIT_PORT}" \
RABBIT_USER="${RABBIT_USER}" \
RABBIT_PASS="${RABBIT_PASS}" \
POLL_INTERVAL="${POLL_INTERVAL}" \
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT}" \
WORK_DIR="${WORK_DIR}" \
"${SUB_RUNNER}" "${M}" 2>&1 | tee "${RUN_DIR}/subscribers_stdout.log" &
subs_pid=$!

sleep 1

set +e
producer_out="$(
  python3 "${PRODUCER_PY}" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --exchange "${EXCHANGE_NAME}" \
    -n "${messages}" \
    --payload-bytes "${SIZE}" \
    --rate "${RATE}" \
    ${PRODUCER_FLAGS} \
    2>&1
)"
rc=$?
set -e

echo "${producer_out}" > "${RUN_DIR}/producer_stdout.log"
touch "${PRODUCER_DONE_FILE}"

if [[ "${rc}" -ne 0 ]]; then
  echo "Producer failed rc=${rc}. See ${RUN_DIR}/producer_stdout.log"
  kill "${subs_pid}" 2>/dev/null || true
  wait "${subs_pid}" 2>/dev/null || true
  exit 1
fi

producer_msgps="$(echo "${producer_out}" | awk -F': ' '/^throughput_msg_per_sec:/ {print $2}' | tail -n1)"
producer_msgps="${producer_msgps:-0}"

wait "${subs_pid}" || true

metrics="$(
  python3 "${SUMMARIZER}" \
    --run-dir "${RUN_DIR}" \
    --pattern "s*.jsonl" \
    --cpu-log "${CPU_LOG}" \
    --mem-log "${MEM_LOG}" \
    --backlog-log "${QUEUE_TS}"
)"

SUMMARY_CSV="${OUT_ROOT}/summary.csv"
if [[ ! -f "${SUMMARY_CSV}" ]]; then
  echo "mode,rate,M,producer_msgps,delivery_throughput_msgps,latency_avg_ms,latency_p95_ms,cpu_avg_pct,cpu_max_pct,mem_avg_mib,mem_max_mib,backlog_avg_msgs,backlog_max_msgs,acked_total,unique_messages,fanout_factor,subscriber_duration_s,prefetch,payload_bytes,exchange,queue_base,run_tag" > "${SUMMARY_CSV}"
fi

echo "${MODE},${RATE},${M},${producer_msgps},${metrics},${PREFETCH},${SIZE},${EXCHANGE_NAME},${QUEUE_BASE},${run_tag}" | tee -a "${SUMMARY_CSV}"

echo
echo "DONE."
echo "Run folder:  ${RUN_DIR}"
echo "Summary CSV: ${SUMMARY_CSV}"