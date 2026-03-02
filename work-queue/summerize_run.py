#!/usr/bin/env python3
import argparse, glob, json, math, os, re, statistics as st

def pct(sorted_vals, p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))

def parse_worker_jsonl(run_dir: str, pattern: str):
    vals_lat = []
    ts_list = []
    count_acked = 0

    files = sorted(glob.glob(os.path.join(run_dir, pattern)))
    for fn in files:
        with open(fn, "r", encoding="utf-8") as f:
            for line in f:
                if not line or line[0] != "{":
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get("acked") is True:
                    count_acked += 1
                    ts = d.get("ts")
                    if isinstance(ts, (int, float)):
                        ts_list.append(float(ts))
                    v = d.get("net_latency_ms")
                    if isinstance(v, (int, float)) and v >= 0:
                        vals_lat.append(float(v))

    ts_list.sort()
    vals_lat.sort()

    if len(ts_list) >= 2:
        dur = ts_list[-1] - ts_list[0]
    else:
        dur = 0.0

    throughput = (count_acked / dur) if dur > 0 else 0.0
    lat_avg = st.mean(vals_lat) if vals_lat else 0.0
    lat_p95 = pct(vals_lat, 0.95)

    return {
        "acked_count": count_acked,
        "worker_duration_s": dur,
        "throughput_msgps": throughput,
        "lat_avg_ms": lat_avg,
        "lat_p95_ms": lat_p95,
        "files": len(files),
        "lat_count": len(vals_lat),
    }

def parse_cpu_log(cpu_log: str):
    vals = []
    try:
        with open(cpu_log, "r", encoding="utf-8") as f:
            for line in f:
                m = re.search(r"([\d.]+)%", line)
                if m:
                    vals.append(float(m.group(1)))
    except FileNotFoundError:
        pass
    if not vals:
        return 0.0, 0.0
    return float(st.mean(vals)), float(max(vals))

def parse_mem_log(mem_log: str):
    """
    mem log format: "<Name> <MemUsage>"
    Ví dụ: "mq-lab-rabbitmq-1 123.4MiB / 7.776GiB"
    Ta lấy số bên trái (usage) và chuẩn hoá về MiB.
    """
    def to_mib(x: float, unit: str) -> float:
        unit = unit.lower()
        if unit == "b":
            return x / (1024 * 1024)
        if unit == "kib":
            return x / 1024
        if unit == "mib":
            return x
        if unit == "gib":
            return x * 1024
        return x

    samples = []
    try:
        with open(mem_log, "r", encoding="utf-8") as f:
            for line in f:
                # find "123.4MiB"
                m = re.search(r"([\d.]+)\s*(B|KiB|MiB|GiB)", line)
                if m:
                    samples.append(to_mib(float(m.group(1)), m.group(2)))
    except FileNotFoundError:
        pass

    if not samples:
        return 0.0, 0.0
    return float(st.mean(samples)), float(max(samples))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--pattern", default="w*.jsonl")
    ap.add_argument("--cpu-log", default="cpu_rabbit.log")
    ap.add_argument("--mem-log", default="mem_rabbit.log")
    args = ap.parse_args()

    w = parse_worker_jsonl(args.run_dir, args.pattern)
    cpu_avg, cpu_max = parse_cpu_log(os.path.join(args.run_dir, args.cpu_log))
    mem_avg, mem_max = parse_mem_log(os.path.join(args.run_dir, args.mem_log))

    # Print as single line CSV-friendly values (no header)
    print(
        f"{w['throughput_msgps']:.2f},"
        f"{w['lat_avg_ms']:.2f},"
        f"{w['lat_p95_ms']:.2f},"
        f"{cpu_avg:.2f},{cpu_max:.2f},"
        f"{mem_avg:.2f},{mem_max:.2f},"
        f"{w['acked_count']},{w['worker_duration_s']:.2f}"
    )

if __name__ == "__main__":
    main()