#!/usr/bin/env python3
import csv
import os
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data')
PLOTS_DIR = os.path.join(ROOT, 'plots')
CSV_PATH = os.path.join(DATA_DIR, 'rdma_normal_fanout_8.csv')
OUT_PATH = os.path.join(PLOTS_DIR, 'rdma_normal_2gb_metrics.png')

os.makedirs(PLOTS_DIR, exist_ok=True)

TARGET_SIZE = 2147483648

with open(CSV_PATH, newline='') as f:
    reader = csv.DictReader(f)
    row = None
    for r in reader:
        try:
            if int(r['size(bytes)']) == TARGET_SIZE:
                row = r
                break
        except Exception:
            continue

if row is None:
    print(f"No row for size {TARGET_SIZE} in {CSV_PATH}")
    raise SystemExit(1)

# Prefer Mbit/s columns for throughput
avg_mbit = float(row.get('avg_send_throughput(MBits/s)', 'nan'))
peak_mbit = float(row.get('peak_send_throughput(MBits/s)', 'nan'))

# Transfer times in ms
avg_ms = float(row.get('avg_send_transfer_time(ms)', 'nan'))
peak_ms = float(row.get('peak_send_transfer_time(ms)', 'nan'))

# Plot
fig, axes = plt.subplots(1, 2, figsize=(10, 4))

# Throughput bar
ax = axes[0]
labels = ['avg', 'peak']
vals = [avg_mbit, peak_mbit]
colors = ['tab:blue', 'tab:orange']
ax.bar(labels, vals, color=colors)
ax.set_title('Send Throughput (Mb/s) - 2GB')
ax.set_ylabel('Mb/s')
for i, v in enumerate(vals):
    ax.text(i, v * 1.02 if v >= 0 else 0, f"{v:.2f}", ha='center')

# Transfer time bar (ms)
ax = axes[1]
vals_t = [avg_ms, peak_ms]
ax.bar(labels, vals_t, color=colors)
ax.set_title('Send Transfer Time (ms) - 2GB')
ax.set_ylabel('ms')
for i, v in enumerate(vals_t):
    ax.text(i, v * 1.02 if v >= 0 else 0, f"{v:.2f}", ha='center')

fig.tight_layout()
fig.savefig(OUT_PATH, dpi=150)
print(f"Wrote: {OUT_PATH}")
