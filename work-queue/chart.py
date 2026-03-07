#!/usr/bin/env python3
import argparse
import math
import re
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


SUMMARY_REQUIRED_COLS = {
    "mode",
    "rate",
    "N",
    "throughput_msgps",
    "latency_p95_ms",
    "cpu_avg_pct",
    "mem_avg_mib",
    "prefetch",
    "payload_bytes",
    "run_tag",
}

NUMERIC_COLS = [
    "rate",
    "N",
    "throughput_msgps",
    "latency_avg_ms",
    "latency_p95_ms",
    "cpu_avg_pct",
    "cpu_max_pct",
    "mem_avg_mib",
    "mem_max_mib",
    "acked_count",
    "worker_duration_s",
    "prefetch",
    "payload_bytes",
    "confirm_fail",
]

CPU_LINE_RE = re.compile(r"^\S+\s+([0-9]+(?:\.[0-9]+)?)%")
MEM_LINE_RE = re.compile(
    r"^\S+\s+([0-9]+(?:\.[0-9]+)?)([KMG]i?B)\s*/\s*([0-9]+(?:\.[0-9]+)?)([KMG]i?B)"
)


def parse_args():
    p = argparse.ArgumentParser(description="Generate charts for work-queue benchmark results")
    p.add_argument(
        "--results-dir",
        required=True,
        help="Path to results/<timestamp> directory, e.g. results/20260306_130431",
    )
    p.add_argument(
        "--summary",
        default=None,
        help="Optional explicit path to summary.csv (default: <results-dir>/summary.csv)",
    )
    p.add_argument(
        "--out-dir",
        default=None,
        help="Optional output charts dir (default: <results-dir>/charts)",
    )
    return p.parse_args()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def smart_read_csv(path: Path) -> pd.DataFrame:
    # Try standard CSV first
    try:
        df = pd.read_csv(path)
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    # Fallback for semicolon-separated files
    try:
        df = pd.read_csv(path, sep=";")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    # Last fallback
    return pd.read_csv(path, engine="python")


def normalize_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col in NUMERIC_COLS:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_summary(summary_path: Path) -> pd.DataFrame:
    df = smart_read_csv(summary_path)
    df.columns = [c.strip() for c in df.columns]
    df = normalize_numeric(df)

    missing = SUMMARY_REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"summary.csv thiếu cột: {sorted(missing)}\n"
            f"Các cột hiện có: {list(df.columns)}"
        )

    df["mode"] = df["mode"].astype(str).str.strip()
    df["run_tag"] = df["run_tag"].astype(str).str.strip()
    return df


def mib_from_value_unit(value: float, unit: str) -> float:
    unit = unit.upper()
    if unit == "KIB":
        return value / 1024.0
    if unit == "MIB":
        return value
    if unit == "GIB":
        return value * 1024.0
    if unit == "KB":
        return value / 1024.0
    if unit == "MB":
        return value
    if unit == "GB":
        return value * 1024.0
    return value


def parse_cpu_log(cpu_log: Path) -> pd.DataFrame:
    rows = []
    if not cpu_log.exists():
        return pd.DataFrame(columns=["sample_idx", "cpu_pct"])

    with cpu_log.open("r", encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            m = CPU_LINE_RE.match(line)
            if not m:
                continue
            rows.append({"sample_idx": idx, "cpu_pct": float(m.group(1))})
    return pd.DataFrame(rows)


def parse_mem_log(mem_log: Path) -> pd.DataFrame:
    rows = []
    if not mem_log.exists():
        return pd.DataFrame(columns=["sample_idx", "mem_used_mib", "mem_total_mib"])

    with mem_log.open("r", encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            m = MEM_LINE_RE.match(line)
            if not m:
                continue
            used_val = float(m.group(1))
            used_unit = m.group(2)
            total_val = float(m.group(3))
            total_unit = m.group(4)
            rows.append(
                {
                    "sample_idx": idx,
                    "mem_used_mib": mib_from_value_unit(used_val, used_unit),
                    "mem_total_mib": mib_from_value_unit(total_val, total_unit),
                }
            )
    return pd.DataFrame(rows)


def load_queue_ts(ts_csv: Path) -> pd.DataFrame:
    if not ts_csv.exists():
        return pd.DataFrame()

    df = smart_read_csv(ts_csv)
    df.columns = [c.strip() for c in df.columns]

    for col in df.columns:
        if col not in ("ts_iso",):
            df[col] = (
                df[col].astype(str).str.strip().str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # hỗ trợ nhiều kiểu tên cột
    ready_col = None
    unacked_col = None
    total_col = None

    for c in df.columns:
        lc = c.lower()
        if lc in ("ready", "ready_total", "messages_ready"):
            ready_col = c
        elif lc in ("unacked", "unacked_total", "messages_unacknowledged"):
            unacked_col = c
        elif lc == "total":
            total_col = c

    if ready_col is None:
        ready_col = "ready"
        df[ready_col] = 0.0
    if unacked_col is None:
        unacked_col = "unacked"
        df[unacked_col] = 0.0
    if total_col is None:
        total_col = "total"
        df[total_col] = df[ready_col].fillna(0) + df[unacked_col].fillna(0)

    if "ts_epoch" in df.columns:
        t0 = df["ts_epoch"].dropna().iloc[0] if not df["ts_epoch"].dropna().empty else 0
        df["t_rel_s"] = df["ts_epoch"] - t0
    else:
        df["t_rel_s"] = range(len(df))

    # producer_alive có thể không có ở work-queue, cứ để None
    if "producer_alive" not in df.columns:
        df["producer_alive"] = None

    return df.rename(
        columns={
            ready_col: "ready_plot",
            unacked_col: "unacked_plot",
            total_col: "total_plot",
        }
    )


def save_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    line_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    out_path: Path,
):
    if df.empty:
        return

    plt.figure(figsize=(10, 6))
    for line_value in sorted(df[line_col].dropna().unique()):
        sub = df[df[line_col] == line_value].sort_values(x_col)
        if sub.empty:
            continue
        plt.plot(sub[x_col], sub[y_col], marker="o", label=f"{line_col}={int(line_value)}")

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def generate_summary_charts(df: pd.DataFrame, out_dir: Path):
    ensure_dir(out_dir)

    metrics = [
        ("throughput_msgps", "throughput", "Throughput (msg/s)"),
        ("latency_p95_ms", "p95", "Latency p95 (ms)"),
        ("cpu_avg_pct", "cpu_avg", "CPU avg (%)"),
        ("mem_avg_mib", "mem_avg", "RAM avg (MiB)"),
    ]

    # 1) metric vs rate, fix mode/payload/N, lines=prefetch
    for mode in sorted(df["mode"].dropna().unique()):
        for payload in sorted(df["payload_bytes"].dropna().unique()):
            for n in sorted(df["N"].dropna().unique()):
                sub = df[
                    (df["mode"] == mode)
                    & (df["payload_bytes"] == payload)
                    & (df["N"] == n)
                ].copy()
                if sub.empty:
                    continue

                for metric_col, metric_name, y_label in metrics:
                    out_path = out_dir / f"mode{mode}_payload{int(payload)}_N{int(n)}_{metric_name}_vs_rate.png"
                    save_line_chart(
                        df=sub,
                        x_col="rate",
                        y_col=metric_col,
                        line_col="prefetch",
                        title=f"Mode {mode}: {metric_name} vs rate (payload={int(payload)} bytes, N={int(n)})",
                        xlabel="Publish rate (msg/s)",
                        ylabel=y_label,
                        out_path=out_path,
                    )

    # 2) metric vs N, fix mode/payload/rate, lines=prefetch
    for mode in sorted(df["mode"].dropna().unique()):
        for payload in sorted(df["payload_bytes"].dropna().unique()):
            for rate in sorted(df["rate"].dropna().unique()):
                sub = df[
                    (df["mode"] == mode)
                    & (df["payload_bytes"] == payload)
                    & (df["rate"] == rate)
                ].copy()
                if sub.empty:
                    continue

                for metric_col, metric_name, y_label in metrics:
                    out_path = out_dir / f"mode{mode}_payload{int(payload)}_rate{int(rate)}_{metric_name}_vs_N.png"
                    save_line_chart(
                        df=sub,
                        x_col="N",
                        y_col=metric_col,
                        line_col="prefetch",
                        title=f"Mode {mode}: {metric_name} vs N (payload={int(payload)} bytes, rate={int(rate)})",
                        xlabel="Number of workers (N)",
                        ylabel=y_label,
                        out_path=out_path,
                    )

    # 3) metric vs prefetch, fix mode/payload/rate, lines=N
    for mode in sorted(df["mode"].dropna().unique()):
        for payload in sorted(df["payload_bytes"].dropna().unique()):
            for rate in sorted(df["rate"].dropna().unique()):
                sub = df[
                    (df["mode"] == mode)
                    & (df["payload_bytes"] == payload)
                    & (df["rate"] == rate)
                ].copy()
                if sub.empty:
                    continue

                for metric_col, metric_name, y_label in metrics:
                    out_path = out_dir / f"mode{mode}_payload{int(payload)}_rate{int(rate)}_{metric_name}_vs_prefetch.png"
                    plt.figure(figsize=(10, 6))
                    for n in sorted(sub["N"].dropna().unique()):
                        sub_n = sub[sub["N"] == n].sort_values("prefetch")
                        plt.plot(sub_n["prefetch"], sub_n[metric_col], marker="o", label=f"N={int(n)}")

                    plt.title(f"Mode {mode}: {metric_name} vs prefetch (payload={int(payload)} bytes, rate={int(rate)})")
                    plt.xlabel("Prefetch")
                    plt.ylabel(y_label)
                    plt.grid(True, alpha=0.4)
                    plt.legend()
                    plt.tight_layout()
                    plt.savefig(out_path, dpi=150)
                    plt.close()


def plot_queue_ts(run_dir: Path, out_dir: Path):
    ts_csv = run_dir / "queue_ts.csv"
    df = load_queue_ts(ts_csv)
    if df.empty:
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df["t_rel_s"], df["ready_plot"], marker="o", label="ready")
    plt.plot(df["t_rel_s"], df["unacked_plot"], marker="o", label="unacked")
    plt.plot(df["t_rel_s"], df["total_plot"], marker="o", label="total")

    if "producer_alive" in df.columns and df["producer_alive"].notna().any():
        alive_idx = df["producer_alive"] == 1
        if alive_idx.any():
            # đánh dấu điểm producer còn chạy
            plt.scatter(
                df.loc[alive_idx, "t_rel_s"],
                df.loc[alive_idx, "total_plot"],
                marker="x",
                label="producer_alive=1",
            )

    plt.title(f"{run_dir.name}: queue backlog over time")
    plt.xlabel("Time since first sample (s)")
    plt.ylabel("Messages")
    plt.grid(True, alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "queue_ts.png", dpi=150)
    plt.close()


def plot_cpu_ts(run_dir: Path, out_dir: Path):
    df = parse_cpu_log(run_dir / "cpu_rabbit.log")
    if df.empty:
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df["sample_idx"], df["cpu_pct"], marker="o")
    plt.title(f"{run_dir.name}: RabbitMQ CPU over time")
    plt.xlabel("Sample index (~1 sample/s)")
    plt.ylabel("CPU (%)")
    plt.grid(True, alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_dir / "cpu_ts.png", dpi=150)
    plt.close()


def plot_mem_ts(run_dir: Path, out_dir: Path):
    df = parse_mem_log(run_dir / "mem_rabbit.log")
    if df.empty:
        return

    plt.figure(figsize=(10, 6))
    plt.plot(df["sample_idx"], df["mem_used_mib"], marker="o", label="used")
    if "mem_total_mib" in df.columns and df["mem_total_mib"].notna().any():
        plt.plot(df["sample_idx"], df["mem_total_mib"], marker="o", label="total")
    plt.title(f"{run_dir.name}: RabbitMQ RAM over time")
    plt.xlabel("Sample index (~1 sample/s)")
    plt.ylabel("Memory (MiB)")
    plt.grid(True, alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "mem_ts.png", dpi=150)
    plt.close()


def generate_per_run_ts_charts(results_dir: Path, summary_df: pd.DataFrame, out_root: Path):
    ts_root = out_root / "timeseries"
    ensure_dir(ts_root)

    for _, row in summary_df.iterrows():
        run_tag = str(row["run_tag"]).strip()
        run_dir = results_dir / run_tag
        if not run_dir.exists():
            continue

        run_out = ts_root / run_tag
        ensure_dir(run_out)

        plot_queue_ts(run_dir, run_out)
        plot_cpu_ts(run_dir, run_out)
        plot_mem_ts(run_dir, run_out)


def main():
    args = parse_args()

    results_dir = Path(args.results_dir).resolve()
    summary_path = Path(args.summary).resolve() if args.summary else results_dir / "summary.csv"
    out_dir = Path(args.out_dir).resolve() if args.out_dir else results_dir / "charts"

    if not results_dir.exists():
        raise FileNotFoundError(f"Không thấy results dir: {results_dir}")
    if not summary_path.exists():
        raise FileNotFoundError(f"Không thấy summary.csv: {summary_path}")

    ensure_dir(out_dir)

    df = load_summary(summary_path)

    # charts tổng hợp
    summary_chart_dir = out_dir / "summary"
    generate_summary_charts(df, summary_chart_dir)

    # charts time-series cho từng run
    generate_per_run_ts_charts(results_dir, df, out_dir)

    print(f"[OK] summary.csv: {summary_path}")
    print(f"[OK] charts saved to: {out_dir}")
    print(f"[OK] summary charts: {summary_chart_dir}")
    print(f"[OK] per-run time-series charts: {out_dir / 'timeseries'}")


if __name__ == "__main__":
    main()