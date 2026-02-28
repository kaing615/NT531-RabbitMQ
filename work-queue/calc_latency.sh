#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${1:-/home/azureuser/work-queue}"
PATTERN="${2:-w*.jsonl}"

python3 - "$WORK_DIR" "$PATTERN" <<'PY'
import glob, json, math, statistics as st, os, sys

work_dir = sys.argv[1]
pattern  = sys.argv[2]

vals = []
files = sorted(glob.glob(os.path.join(work_dir, pattern)))

for fn in files:
    try:
        with open(fn, "r", encoding="utf-8") as f:
            for line in f:
                if '"net_latency_ms"' not in line:
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                v = d.get("net_latency_ms")
                if isinstance(v, (int, float)) and v >= 0:
                    vals.append(float(v))
    except FileNotFoundError:
        pass

vals.sort()
avg = st.mean(vals) if vals else 0.0

def pct(p: float) -> float:
    if not vals:
        return 0.0
    k = (len(vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return vals[int(k)]
    return vals[f] + (vals[c] - vals[f]) * (k - f)

p95 = pct(0.95)

print(f"files={len(files)} count={len(vals)} avg_ms={avg:.2f} p95_ms={p95:.2f}")
PY
