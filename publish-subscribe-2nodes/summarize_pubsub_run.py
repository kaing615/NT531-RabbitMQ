#!/usr/bin/env python3
import argparse
import glob
import json
import math
import os
import re
import statistics as st


def pct(sorted_vals, p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))



def parse_subscriber_jsonl(run_dir: str, pattern: str):
    vals_lat = []
    ts_list = []
    acked_total = 0
    uniq = set()

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
                    acked_total += 1
                    seq = d.get("seq")
                    if isinstance(seq, int):
                        uniq.add(seq)
                    ts = d.get("ts")
                    if isinstance(ts, (int, float)):
                        ts_list.append(float(ts))
                    v = d.get("net_latency_ms")
                    if isinstance(v, (int, float)) and v >= 0:
                        vals_lat.append(float(v))

    ts_list.sort()
    vals_lat.sort()
    dur = (ts_list[-1] - ts_list[0]) if len(ts_list) >= 2 else 0.0
    delivery_throughput = (acked_total / dur) if dur > 0 else 0.0
    lat_avg = st.mean(vals_lat) if vals_lat else 0.0
    lat_p95 = pct(vals_lat, 0.95)
    unique_messages = len(uniq)
    fanout_factor = (acked_total / unique_messages) if unique_messages > 0 else 0.0

    return {
        "acked_total": acked_total,
        "subscriber_duration_s": dur,
        "delivery_throughput_msgps": delivery_throughput,
        "lat_avg_ms": lat_avg,
        "lat_p95_ms": lat_p95,
        "unique_messages": unique_messages,
        "fanout_factor": fanout_factor,
    }



def parse_cpu_log(cpu_log: str):
    samples = []
    try:
        with open(cpu_log, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    try:
                        samples.append(float(parts[1]) + float(parts[2]))
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    if not samples:
        return 0.0, 0.0
    return float(st.mean(samples)), float(max(samples))



def parse_mem_log(mem_log: str):
    def to_mib(mem_str: str) -> float:
        mem_str = mem_str.strip()
        m = re.search(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)", mem_str, re.IGNORECASE)
        if not m:
            return 0.0
        val = float(m.group(1))
        unit = m.group(2).lower()
        if unit == "b":
            return val / (1024 * 1024)
        if unit == "kib":
            return val / 1024
        if unit == "mib":
            return val
        if unit == "gib":
            return val * 1024
        if unit == "tib":
            return val * 1024 * 1024
        return val

    samples = []
    try:
        with open(mem_log, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    samples.append(to_mib(parts[1]) + to_mib(parts[2]))
    except FileNotFoundError:
        pass
    if not samples:
        return 0.0, 0.0
    return float(st.mean(samples)), float(max(samples))



def parse_backlog_log(backlog_log: str):
    totals = []
    try:
        with open(backlog_log, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 2:
                    try:
                        totals.append(float(parts[-1]))
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    if not totals:
        return 0.0, 0.0
    return float(st.mean(totals)), float(max(totals))



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--pattern", default="s*.jsonl")
    ap.add_argument("--cpu-log", default="cpu_rabbit.log")
    ap.add_argument("--mem-log", default="mem_rabbit.log")
    ap.add_argument("--backlog-log", default="backlog.log")
    args = ap.parse_args()

    s = parse_subscriber_jsonl(args.run_dir, args.pattern)
    cpu_avg, cpu_max = parse_cpu_log(os.path.join(args.run_dir, args.cpu_log))
    mem_avg, mem_max = parse_mem_log(os.path.join(args.run_dir, args.mem_log))
    backlog_avg, backlog_max = parse_backlog_log(os.path.join(args.run_dir, args.backlog_log))

    print(
        f"{s['delivery_throughput_msgps']:.2f},"
        f"{s['lat_avg_ms']:.2f},"
        f"{s['lat_p95_ms']:.2f},"
        f"{cpu_avg:.2f},{cpu_max:.2f},"
        f"{mem_avg:.2f},{mem_max:.2f},"
        f"{backlog_avg:.2f},{backlog_max:.2f},"
        f"{s['acked_total']},{s['unique_messages']},{s['fanout_factor']:.2f},{s['subscriber_duration_s']:.2f}"
    )


if __name__ == "__main__":
    main()
