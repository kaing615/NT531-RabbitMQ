import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results.csv")  # cột ví dụ: workers, rate, ack, p95_ms

# Throughput vs rate
for w in sorted(df["workers"].unique()):
    d = df[df["workers"] == w].sort_values("rate")
    plt.plot(d["rate"], d["ack"], label=f"N={w}")
plt.xlabel("publish rate (msg/s)")
plt.ylabel("throughput (ack msg/s)")
plt.legend()
plt.show()

# Latency p95 vs rate
for w in sorted(df["workers"].unique()):
    d = df[df["workers"] == w].sort_values("rate")
    plt.plot(d["rate"], d["p95_ms"], label=f"N={w}")
plt.xlabel("publish rate (msg/s)")
plt.ylabel("latency p95 (ms)")
plt.legend()
plt.show()