import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# === 1️⃣ 读取完整数据（不指定 names，自动读全部列） ===
df = pd.read_csv("data/results/timings_summary.csv", header=None)

# 确认列数
print("[INFO] Detected columns:", df.shape[1])

# === 2️⃣ 重命名列 ===
df.columns = [
    "method", "dataset", "threshold",
    "t_read", "t_popularity", "t_compute", "t_write", "t_total"
]

# === 3️⃣ 提取 dataset size ===
df["dataset"] = df["dataset"].astype(str)
df["rows"] = (
    df["dataset"]
    .str.extract(r"R_(\d+)k", expand=False)
    .astype(float)
    * 1000
)

# === 4️⃣ 绘制总运行时间 ===
plt.figure(figsize=(7, 5))
for method in df["method"].unique():
    subset = df[df["method"] == method].sort_values("rows")
    plt.plot(subset["rows"], subset["t_total"], marker="o", label=method)

plt.xlabel("Dataset size (rows)")
plt.ylabel("Total Runtime (seconds)")
plt.title("Total Runtime Comparison Across Methods")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
