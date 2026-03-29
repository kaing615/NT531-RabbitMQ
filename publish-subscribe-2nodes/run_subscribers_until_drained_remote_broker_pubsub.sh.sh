#!/usr/bin/env bash
set -euo pipefail

M="${1:-3}"

WORK_DIR="${WORK_DIR:-${HOME}/pubsub-baseline}"
SUBSCRIBER_PY="${SUBSCRIBER_PY:-${WORK_DIR}/subscriber.py}"

OUTPUT_DIR="${OUTPUT_DIR:-.}"
CPU_LOG="${CPU_LOG:-cpu_rabbit.log}"
MEM_LOG="${MEM_LOG:-mem_rabbit.log}"
QUEUE_TS="${QUEUE_TS:-queue_ts.csv}"
PRODUCER_DONE_FILE="${PRODUCER_DONE_FILE:-}"

RABBIT_HOST="${RABBIT_HOST:?set RABBIT_HOST to node2 broker IP/DNS}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

EXCHANGE_NAME="${EXCHANGE_NAME:-events_fanout_A}"
QUEUE_BASE="${QUEUE_BASE:-events_sub_A_}"
PREFETCH="${PREFETCH:-10}"
SLEEP_MS="${SLEEP_MS:-20}"
DURABLE="${DURABLE:-0}"

BROKER_SSH="${BROKER_SSH:?set BROKER_SSH like azureuser@10.0.0.5}"
BROKER_RMQCTL="${BROKER_RMQCTL:-sudo rabbitmqctl}"
BROKER_CPU_CMD="${BROKER_CPU_CMD:-ps -C beam.smp -o %cpu= | awk '{s+=\$1} END {print s+0}'}"
BROKER_MEM_CMD="${BROKER_MEM_CMD:-ps -C beam.smp -o rss= | awk '{s+=\$1} END {print s/1024}'}"

POLL_INTERVAL="${POLL_INTERVAL:-3}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

mkdir -p "${OUTPUT_DIR}"

broker_exec() {
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${BROKER_SSH}" "$@"
}

get_broker_cpu() {
  broker_exec "${BROKER_CPU_CMD}" 2>/dev/null || echo "0"
}

get_broker_mem() {
  broker_exec "${BROKER_MEM_CMD}" 2>/dev/null || echo "0"
}

list_queues_remote() {
  broker_exec "${BROKER_RMQCTL} list_queues -q name messages_ready messages_unacknowledged 2>/dev/null" || true
}

echo "[Subscribers] Starting ${M} subscribers on node1; broker is on node2 (${RABBIT_HOST})..."

declare -a PIDS
QUEUE_NAMES=()

cleanup() {
  for p in "${PIDS[@]:-}"; do
    kill "${p}" 2>/dev/null || true
  done
  wait || true
}
trap cleanup EXIT

for i in $(seq 1 "${M}"); do
  q="${QUEUE_BASE}${i}"
  QUEUE_NAMES+=("${q}")

  python3 "${SUBSCRIBER_PY}" \
    --host "${RABBIT_HOST}" --port "${RABBIT_PORT}" \
    --user "${RABBIT_USER}" --password "${RABBIT_PASS}" \
    --exchange "${EXCHANGE_NAME}" \
    --queue "${q}" \
    --durable "${DURABLE}" \
    --subscriber-id "${i}" \
    --prefetch "${PREFETCH}" \
    --sleep-ms "${SLEEP_MS}" \
    --log "${OUTPUT_DIR}/s${i}.jsonl" \
    > "${OUTPUT_DIR}/sub${i}.out.log" 2>&1 &
  PIDS+=("$!")
done

echo "[Subscribers] Started. Monitoring remote broker metrics..."

echo "ts_epoch,cpu_node1,cpu_node2" > "${OUTPUT_DIR}/${CPU_LOG}"
echo "ts_epoch,mem_node1,mem_node2" > "${OUTPUT_DIR}/${MEM_LOG}"
echo "ts_iso,ts_epoch,ready_total,unacked_total,total" > "${OUTPUT_DIR}/${QUEUE_TS}"

zero_ok=0

while true; do
  ts_iso="$(TZ=Asia/Ho_Chi_Minh date '+%Y-%m-%d %H:%M:%S')"
  ts_epoch="$(date +%s)"

  cpu2="$(get_broker_cpu)"
  mem2="$(get_broker_mem)"

  # Giữ format 3 cột để summarize_pubsub_run.py dùng lại được
  echo "${ts_epoch},0,${cpu2}" >> "${OUTPUT_DIR}/${CPU_LOG}"
  echo "${ts_epoch},0MiB,${mem2}MiB" >> "${OUTPUT_DIR}/${MEM_LOG}"

  queue_stats="$(list_queues_remote)"

  ready_total=0
  unacked_total=0

  for q in "${QUEUE_NAMES[@]}"; do
    read -r r u < <(
      echo "${queue_stats}" | awk -v q="${q}" '
        $1==q {print $2, $3; found=1}
        END {if(!found) print "0 0"}'
    )
    r="${r:-0}"
    u="${u:-0}"
    ready_total=$((ready_total + r))
    unacked_total=$((unacked_total + u))
  done

  total=$((ready_total + unacked_total))

  echo "${ts_iso},${ts_epoch},${ready_total},${unacked_total},${total}" >> "${OUTPUT_DIR}/${QUEUE_TS}"
  echo "[${ts_iso}] ready=${ready_total} unacked=${unacked_total} total=${total}"

  alive_count=0
  for p in "${PIDS[@]}"; do
    if kill -0 "${p}" 2>/dev/null; then
      alive_count=$((alive_count + 1))
    fi
  done

  if [[ "${alive_count}" -eq 0 && "${total}" -gt 0 ]]; then
    echo "[Subscribers] WARNING: all subscribers died but backlog still ${total}."
    break
  fi

  if [[ "${total}" -eq 0 ]]; then
    zero_ok=$((zero_ok + 1))
  else
    zero_ok=0
  fi

  if [[ -n "${PRODUCER_DONE_FILE}" && -f "${PRODUCER_DONE_FILE}" && "${zero_ok}" -ge "${STABLE_ZERO_COUNT}" ]]; then
    echo "[Subscribers] All queues drained."
    break
  fi

  sleep "${POLL_INTERVAL}"
done

echo "[Subscribers] Done."