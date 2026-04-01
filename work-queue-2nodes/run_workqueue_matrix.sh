#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${WORK_DIR:-${HOME}/NT531-RabbitMQ/work-queue}"

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
  echo
  echo "Example:"
  echo "  BROKER_SSH_HOST=10.0.2.15 BROKER_SSH_USER=ubuntu BROKER_CONTAINER=rabbitmq ./run_workqueue_matrix.sh"
  exit 1
fi

# ===== RabbitMQ connection for producer/workers =====
RABBIT_HOST="${RABBIT_HOST:-}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-admin}"
RABBIT_PASS="${RABBIT_PASS:-admin123}"

if [[ -z "${RABBIT_HOST}" ]]; then
  echo "ERROR: Missing RABBIT_HOST"
  echo "Example: RABBIT_HOST=10.0.2.15 ./run_workqueue_matrix.sh"
  exit 1
fi

PRODUCER_PY="${PRODUCER_PY:-${WORK_DIR}/producer.py}"
WORKER_RUNNER="${WORKER_RUNNER:-${WORK_DIR}/run_workers_until_drained.sh}"
SUMMARIZER="${SUMMARIZER:-${WORK_DIR}/summarize_run.py}"

# ===== Matrix =====
RATES=(${RATES:-500 1000 2000})
WORKERS_LIST=(${WORKERS_LIST:-1 2 4})
PREFETCH_LIST=(${PREFETCH_LIST:-1 5 10})
SIZES=(${SIZES:-1024 10240})
RUN_SECONDS="${RUN_SECONDS:-20}"
SLEEP_MS="${SLEEP_MS:-20}"

CPU_DELAY="${CPU_DELAY:-3}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"
STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT:-3}"

# ===== Modes =====
# A: non-durable queue, non-persistent msg, confirms OFF
# B: durable queue, persistent msg, confirms OFF
# C: durable queue, persistent msg, confirms ON
MODES=(${MODES:-A B C})

TS="$(TZ=Asia/Ho_Chi_Minh date '+%Y%m%d_%H%M%S')"
OUT_ROOT="${OUT_ROOT:-${WORK_DIR}/results/${TS}}"
RESULT_CSV="${RESULT_CSV:-${OUT_ROOT}/summary.csv}"
mkdir -p "${OUT_ROOT}"

remote_sh() {
  ssh -o BatchMode=yes -o ConnectTimeout=5 "${BROKER_SSH_USER}@${BROKER_SSH_HOST}" "$@"
}

echo "== Check SSH to broker ${BROKER_SSH_USER}@${BROKER_SSH_HOST} =="
remote_sh "hostname" >/dev/null

purge_queue() {
  local q="$1"

  # Ưu tiên purge qua docker exec trong container RabbitMQ
  remote_sh "docker exec ${BROKER_CONTAINER} rabbitmqctl purge_queue ${q}" >/dev/null 2>&1 \
    || remote_sh "rabbitmqctl purge_queue ${q}" >/dev/null 2>&1 \
    || true
}

mode_cfg() {
  # outputs: queue_durable|producer_flags|queue_name
  local mode="$1"
  local base="${QUEUE_BASE:-orders_queue}"
  case "${mode}" in
    A)
      echo "0||${base}_A"
      ;;
    B)
      echo "1|--durable --persistent|${base}_B"
      ;;
    C)
      echo "1|--durable --persistent --confirm|${base}_C"
      ;;
    *)
      echo "Unknown mode: ${mode}" >&2
      exit 1
      ;;
  esac
}

echo "mode,rate,N,throughput_msgps,latency_avg_ms,latency_p95_ms,cpu_avg_pct,cpu_max_pct,mem_avg_mib,mem_max_mib,acked_count,worker_duration_s,prefetch,payload_bytes,confirm_fail,queue,run_tag" \
  > "${RESULT_CSV}"

for mode in "${MODES[@]}"; do
  IFS="|" read -r queue_durable producer_flags queue_name < <(mode_cfg "${mode}")

  for rate in "${RATES[@]}"; do
    for n in "${WORKERS_LIST[@]}"; do
      for prefetch in "${PREFETCH_LIST[@]}"; do
        for size in "${SIZES[@]}"; do

          messages=$((rate * RUN_SECONDS))
          run_tag="mode${mode}_rate${rate}_N${n}_pref${prefetch}_sz${size}_t${RUN_SECONDS}"
          run_dir="${OUT_ROOT}/${run_tag}"
          mkdir -p "${run_dir}"

          cpu_log="${run_dir}/cpu_rabbit.log"
          mem_log="${run_dir}/mem_rabbit.log"

          echo
          echo "===== RUN ${run_tag} queue=${queue_name} durable=${queue_durable} messages=${messages} ====="

          purge_queue "${queue_name}"

          # 1) start workers + logger (wait until queue drained)
          OUTPUT_DIR="${run_dir}" \
          CPU_LOG="${cpu_log}" \
          MEM_LOG="${mem_log}" \
          QUEUE_NAME="${queue_name}" \
          QUEUE_DURABLE="${queue_durable}" \
          PREFETCH="${prefetch}" \
          SLEEP_MS="${SLEEP_MS}" \
          RABBIT_HOST="${RABBIT_HOST}" \
          RABBIT_PORT="${RABBIT_PORT}" \
          RABBIT_USER="${RABBIT_USER}" \
          RABBIT_PASS="${RABBIT_PASS}" \
          BROKER_SSH_HOST="${BROKER_SSH_HOST}" \
          BROKER_SSH_USER="${BROKER_SSH_USER}" \
          BROKER_CONTAINER="${BROKER_CONTAINER}" \
          CPU_DELAY="${CPU_DELAY}" \
          POLL_INTERVAL="${POLL_INTERVAL}" \
          STABLE_ZERO_COUNT="${STABLE_ZERO_COUNT}" \
          WORK_DIR="${WORK_DIR}" \
          "${WORKER_RUNNER}" "${n}" 2>&1 | tee "${run_dir}/workers_stdout.log" &
          workers_pid=$!

          sleep 1

          # 2) run producer
          set +e
          producer_out="$(
            python3 "${PRODUCER_PY}" \
              --host "${RABBIT_HOST}" \
              --port "${RABBIT_PORT}" \
              --user "${RABBIT_USER}" \
              --password "${RABBIT_PASS}" \
              --queue "${queue_name}" \
              -n "${messages}" \
              --payload-bytes "${size}" \
              --rate "${rate}" \
              ${producer_flags} \
              2>&1
          )"
          rc=$?
          set -e

          echo "${producer_out}" > "${run_dir}/producer_stdout.log"

          if [[ "${rc}" -ne 0 ]]; then
            echo "Producer failed rc=${rc}, skip. Check ${run_dir}/producer_stdout.log"
            kill "${workers_pid}" 2>/dev/null || true
            wait "${workers_pid}" 2>/dev/null || true
            continue
          fi

          confirm_fail="$(echo "${producer_out}" | awk -F': ' '/^confirm_fail:/ {print $2}' | tail -n1)"
          confirm_fail="${confirm_fail:-0}"

          # 3) wait drained
          wait "${workers_pid}" || true

          # 4) summarize
          metrics="$(
            python3 "${SUMMARIZER}" \
              --run-dir "${run_dir}" \
              --pattern "w*.jsonl" \
              --cpu-log "cpu_rabbit.log" \
              --mem-log "mem_rabbit.log"
          )"

          echo "${mode},${rate},${n},${metrics},${prefetch},${size},${confirm_fail},${queue_name},${run_tag}" \
            >> "${RESULT_CSV}"

        done
      done
    done
  done
done

echo
echo "DONE: ${RESULT_CSV}"