import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# ---------- Plot helpers ----------
def _plot_lines_by_prefetch(
    df_mode_payload: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    out_path: Path,
):
    plt.figure()
    for pref in sorted(df_mode_payload["prefetch"].dropna().unique()):
        s = df_mode_payload[df_mode_payload["prefetch"] == pref].sort_values(x_col)
        plt.plot(s[x_col], s[y_col], marker="o", label=f"prefetch={int(pref)}")

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


def generate_mode_figs(df: pd.DataFrame, mode: str, out_dir: Path):
    d = df[df["mode"].astype(str).str.strip().str.upper() == mode].copy()
    if d.empty:
        return

    payloads = sorted(d["payload_bytes"].dropna().unique())

    # thr_total
    for payload in payloads:
        dp = d[d["payload_bytes"] == payload]
        _plot_lines_by_prefetch(
            dp,
            x_col="rate",
            y_col="thr_total",
            title=f"Mode {mode} Fanout: thr_total vs rate (payload={int(payload)} bytes)",
            xlabel="Publish rate (msg/s)",
            ylabel="Throughput total thr_total (msg/s)",
            out_path=out_dir / f"thr_total_payload{int(payload)}.png",
        )

    # p95_total (ms -> seconds)
    d["p95_total_s"] = d["p95_total"] / 1000.0
    for payload in payloads:
        dp = d[d["payload_bytes"] == payload]
        _plot_lines_by_prefetch(
            dp,
            x_col="rate",
            y_col="p95_total_s",
            title=f"Mode {mode} Fanout: p95_total vs rate (payload={int(payload)} bytes)",
            xlabel="Publish rate (msg/s)",
            ylabel="p95_total (seconds)",
            out_path=out_dir / f"p95_total_payload{int(payload)}.png",
        )

    # cpu_avg
    for payload in payloads:
        dp = d[d["payload_bytes"] == payload]
        _plot_lines_by_prefetch(
            dp,
            x_col="rate",
            y_col="cpu_avg_pct",
            title=f"Mode {mode} Fanout: CPU avg vs rate (payload={int(payload)} bytes)",
            xlabel="Publish rate (msg/s)",
            ylabel="CPU avg (%)",
            out_path=out_dir / f"cpu_avg_payload{int(payload)}.png",
        )

    # Optional: goodput + mem (nếu có cột)
    if "goodput_MBps" in d.columns:
        for payload in payloads:
            dp = d[d["payload_bytes"] == payload]
            _plot_lines_by_prefetch(
                dp,
                x_col="rate",
                y_col="goodput_MBps",
                title=f"Mode {mode} Fanout: Goodput vs rate (payload={int(payload)} bytes)",
                xlabel="Publish rate (msg/s)",
                ylabel="Goodput (MB/s)",
                out_path=out_dir / f"goodput_payload{int(payload)}.png",
            )

    if "mem_avg_mib" in d.columns:
        for payload in payloads:
            dp = d[d["payload_bytes"] == payload]
            _plot_lines_by_prefetch(
                dp,
                x_col="rate",
                y_col="mem_avg_mib",
                title=f"Mode {mode} Fanout: RAM avg vs rate (payload={int(payload)} bytes)",
                xlabel="Publish rate (msg/s)",
                ylabel="RAM avg (MiB)",
                out_path=out_dir / f"mem_avg_payload{int(payload)}.png",
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to summary.csv")
    ap.add_argument("--out", default="ps_figs_by_mode", help="Output root folder")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    out_root = Path(args.out)

    df = pd.read_csv(csv_path)

    # Ensure numeric columns
    num_cols = ["rate", "prefetch", "payload_bytes", "thr_total", "p95_total", "cpu_avg_pct", "goodput_MBps", "mem_avg_mib"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Detect modes
    if "mode" not in df.columns:
        raise ValueError("summary.csv does not have 'mode' column")

    modes = sorted(df["mode"].dropna().astype(str).str.strip().str.upper().unique().tolist())
    print("Detected modes:", modes)

    for mode in modes:
        mode_dir = out_root / f"mode{mode}"
        generate_mode_figs(df, mode, mode_dir)

    print("DONE. Images saved in:", out_root.resolve())


if __name__ == "__main__":
    main()