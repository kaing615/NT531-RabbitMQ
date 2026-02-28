#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${HOME}/publish-subscribe"
EXCHANGE="${EXCHANGE:-broadcast_x}"
QUEUES=("subq_1" "subq_2" "subq_3")     # M=3 fixed
PREFETCH="${PREFETCH:-10}"
SLEEP_MS="${SLEEP_MS:-20}"

CONTAINER_NAME="${CONTAINER_NAME:-mq-lab-rabbitmq-1}"
CPU_LOG="${CPU_LOG:-cpu_rabbit.log}"
CPU_DELAY="${CPU_DELAY:-10}"                # delay rồi mới log CPU
POLL_INTERVAL="${POLL_INTERVAL:-2}"          # poll queue state
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"  # 0 ổn định N lần mới stop

usage() {
  cat <<EOF2
Usage:
  ./run_ps_baseline.sh

Env (optional):
  EXCHANGE (default: ${EXCHANGE})
  PREFETCH (default: ${PREFETCH})
  SLEEP_MS (default: ${SLEEP_MS})
  CONTAINER_NAME (default: ${CONTAINER_NAME})
  CPU_DELAY (default: ${CPU_DELAY})
  POLL_INTERVAL (default: ${POLL_INTERVAL})
  STABLE_ZERO_COUNT (default: ${STABLE_ZERO_COUNT})
EOF2
}

cd "${WORK_DIR}"

# Activate venv
if [[ -f "${WORK_DIR}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "${WORK_DIR}/.venv/bin/activate"
fi

# Cleanup old logs
rm -f "${WORK_DIR}"/s*.jsonl || true
rm -f "${WORK_DIR}/${CPU_LOG}" || true

SUB_PIDS=()
CPU_PID=""

cleanup() {
  echo
  echo "== Stopping CPU logger =="
  [[ -n "${CPU_PID}" ]] && kill "${CPU_PID}" 2>/dev/null || true

  echo "== Stopping subscribers =="
  for pid in "${SUB_PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done

  echo "== CPU avg/max from ${CPU_LOG} =="
  python3 - <<PY
import re, statistics as st, sys
vals=[]
try:
    for line in open("${WORK_DIR}/${CPU_LOG}", "r", encoding="utf-8"):
        m=re.search(r"([0-9.]+)%", line)
        if m: vals.append(float(m.group(1)))
except FileNotFoundError:
    pass
if not vals:
    print("No CPU samples found.")
else:
    print("samples", len(vals), "avg_cpu%", round(st.mean(vals),2), "max_cpu%", round(max(vals),2))
PY

  echo "== Latency summary (s1/s2/s3 + total) =="
  "${WORK_DIR}/calc_latency_ps.sh" "${WORK_DIR}" "s*.jsonl" || true
}
trap cleanup EXIT

echo "== (1) Start 3 subscribers (M=3) =="
python3 "${WORK_DIR}/sub_fanout.py" --exchange "${EXCHANGE}" --queue "${QUEUES[0]}" --sub-id 1 --prefetch "${PREFETCH}" --sleep-ms "${SLEEP_MS}" --log "${WORK_DIR}/s1.jsonl" >/dev/null 2>&1 &
SUB_PIDS+=("$!")
python3 "${WORK_DIR}/sub_fanout.py" --exchange "${EXCHANGE}" --queue "${QUEUES[1]}" --sub-id 2 --prefetch "${PREFETCH}" --sleep-ms "${SLEEP_MS}" --log "${WORK_DIR}/s2.jsonl" >/dev/null 2>&1 &
SUB_PIDS+=("$!")
python3 "${WORK_DIR}/sub_fanout.py" --exchange "${EXCHANGE}" --queue "${QUEUES[2]}" --sub-id 3 --prefetch "${PREFETCH}" --sleep-ms "${SLEEP_MS}" --log "${WORK_DIR}/s3.jsonl" >/dev/null 2>&1 &
SUB_PIDS+=("$!")

echo "Subscriber PIDs: ${SUB_PIDS[*]}"
echo "Logs: s1.jsonl s2.jsonl s3.jsonl"

echo "== (2) Wait ${CPU_DELAY}s then start CPU sampling =="
sleep "${CPU_DELAY}"

(
  while true; do
    docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" "${CONTAINER_NAME}" >> "${WORK_DIR}/${CPU_LOG}" || true
    sleep 1
  done
) &
CPU_PID="$!"
echo "CPU logger PID: ${CPU_PID}"

# Read ready/unacked for a specific queue via rabbitmqctl in container
get_ready_unacked() {
  local q="$1"
  docker exec "${CONTAINER_NAME}" rabbitmqctl list_queues -q name messages_ready messages_unacknowledged 2>/dev/null \
    | awk -v q="${q}" '$1==q {print $2, $3; found=1} END{if(!found) print "0 0"}'
}

echo "== (3) Waiting until ALL queues drained (ready=0 & unacked=0) =="
echo "NOTE: script will not exit until it first observes some activity (ready/unacked > 0)."

seen_work=0
zero_ok=0

while true; do
  ts="$(date '+%H:%M:%S')"

  all_ready=0
  all_unacked=0
  any_nonzero=0

  line="[${ts}]"
  for q in "${QUEUES[@]}"; do
    read -r ready unacked < <(get_ready_unacked "${q}")
    ready="${ready:-0}"; unacked="${unacked:-0}"
    line+=" ${q}:r=${ready},u=${unacked}"
    all_ready=$((all_ready + ready))
    all_unacked=$((all_unacked + unacked))
    if [[ "${ready}" != "0" || "${unacked}" != "0" ]]; then
      any_nonzero=1
    fi
  done
  echo "${line}"

  # once we see any activity, allow completion condition
  if [[ "${any_nonzero}" == "1" ]]; then
    seen_work=1
  fi

  if [[ "${seen_work}" == "1" && "${all_ready}" == "0" && "${all_unacked}" == "0" ]]; then
    zero_ok=$((zero_ok+1))
  else
    zero_ok=0
  fi

  if [[ "${zero_ok}" -ge "${STABLE_ZERO_COUNT}" ]]; then
    echo "== Drained confirmed for all queues (stable ${STABLE_ZERO_COUNT} polls). Exiting =="
    exit 0
  fi

  sleep "${POLL_INTERVAL}"
done
