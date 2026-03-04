import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ====== CONFIG ======
CSV_PATH = "/Users/dtam.21/Code/NT531/publish-subscribe/results-b/summary.csv"          # đổi path nếu file nằm chỗ khác
OUT_DIR = Path("ps_modeB_figs")   # folder chứa ảnh xuất ra
OUT_DIR.mkdir(parents=True, exist_ok=True)
    
# ====== LOAD DATA ======
df = pd.read_csv(CSV_PATH)

if "mode" in df.columns:
    df = df[df["mode"].astype(str).str.strip().str.upper() == "B"].copy()

# ép kiểu số (tránh Pivot/Excel bị text)
for c in ["rate", "prefetch", "payload_bytes", "thr_total", "p95_total", "cpu_avg_pct"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# đổi p95_total từ ms -> s cho dễ nhìn (vì bạn đang có p95 rất lớn)
df["p95_total_s"] = df["p95_total"] / 1000.0

# ====== PLOT HELPERS ======
def plot_thr_total(payload_bytes: int, out_name: str):
    plt.figure()
    sub = df[df["payload_bytes"] == payload_bytes]
    for pref in sorted(sub["prefetch"].dropna().unique()):
        s = sub[sub["prefetch"] == pref].sort_values("rate")
        plt.plot(s["rate"], s["thr_total"], marker="o", label=f"prefetch={int(pref)}")
    plt.xlabel("Publish rate (msg/s)")
    plt.ylabel("Throughput total thr_total (msg/s)")
    plt.title(f"Mode B Fanout: thr_total vs rate (payload={payload_bytes} bytes)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / out_name, dpi=200)
    plt.close()

def plot_p95_total(payload_bytes: int, out_name: str):
    plt.figure()
    sub = df[df["payload_bytes"] == payload_bytes]
    for pref in sorted(sub["prefetch"].dropna().unique()):
        s = sub[sub["prefetch"] == pref].sort_values("rate")
        plt.plot(s["rate"], s["p95_total_s"], marker="o", label=f"prefetch={int(pref)}")
    plt.xlabel("Publish rate (msg/s)")
    plt.ylabel("p95_total (seconds)")
    plt.title(f"Mode B Fanout: p95_total vs rate (payload={payload_bytes} bytes)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / out_name, dpi=200)
    plt.close()

def plot_cpu_avg(payload_bytes: int, out_name: str):
    plt.figure()
    sub = df[df["payload_bytes"] == payload_bytes]
    for pref in sorted(sub["prefetch"].dropna().unique()):
        s = sub[sub["prefetch"] == pref].sort_values("rate")
        plt.plot(s["rate"], s["cpu_avg_pct"], marker="o", label=f"prefetch={int(pref)}")
    plt.xlabel("Publish rate (msg/s)")
    plt.ylabel("CPU avg (%)")
    plt.title(f"Mode B Fanout: CPU avg vs rate (payload={payload_bytes} bytes)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / out_name, dpi=200)
    plt.close()

# ====== GENERATE 5 FIGURES ======
plot_thr_total(1024,  "fig1_thr_total_rate_payload1KB.png")
plot_thr_total(10240, "fig2_thr_total_rate_payload10KB.png")
plot_p95_total(1024,  "fig3_p95_total_rate_payload1KB.png")
plot_p95_total(10240, "fig4_p95_total_rate_payload10KB.png")
plot_cpu_avg(1024,    "fig5_cpu_avg_rate_payload1KB.png")

print("DONE. Images saved in:", OUT_DIR.resolve())