import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results.csv")  # workers, rate, ack, p95_s
df["p95_ms"] = df["p95_s"] * 1000

# --- Throughput vs rate ---
plt.figure()
for w in sorted(df["workers"].unique()):
    d = df[df["workers"] == w].sort_values("rate")
    plt.plot(d["rate"], d["ack"], marker="o", label=f"N={w}")
plt.xlabel("publish rate (msg/s)")
plt.ylabel("throughput (ack msg/s)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("throughput_vs_rate.png", dpi=200)
plt.show()

# --- Latency p95 vs rate (seconds) ---
plt.figure()
for w in sorted(df["workers"].unique()):
    d = df[df["workers"] == w].sort_values("rate")
    plt.plot(d["rate"], d["p95_s"], marker="o", label=f"N={w}")
plt.xlabel("publish rate (msg/s)")
plt.ylabel("latency p95 (s)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("p95_vs_rate.png", dpi=200)
plt.show()