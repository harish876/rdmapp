#!/usr/bin/env python3
"""
Generate overlay graphs for send-side metrics from rdma_normal.csv and rdma_message_batch.csv:
- avg_send_throughput (Mbits/s)
- peak_send_throughput (Mbits/s)
- avg_send_transfer_time (ms)
- peak_send_transfer_time (ms)

Outputs PNG files into ../plots/
"""
import csv
import os
from typing import Dict, List
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data')
PLOTS_DIR = os.path.join(ROOT, 'plots')
NORMAL_CSV = os.path.join(DATA_DIR, 'rdma_normal.csv')
BATCH_CSV = os.path.join(DATA_DIR, 'rdma_message_batch.csv')

os.makedirs(PLOTS_DIR, exist_ok=True)

# Columns of interest
COLS = {
    'size': 'size(bytes)',
    'avg_send_mbits': 'avg_send_throughput(MBits/s)',
    'peak_send_mbits': 'peak_send_throughput(MBits/s)',
    'avg_send_ms': 'avg_send_transfer_time(ms)',
    'peak_send_ms': 'peak_send_transfer_time(ms)',
}


def read_csv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for r in reader:
            # skip incomplete rows (size present but missing metrics)
            if not r.get(COLS['size']):
                continue
            rows.append(r)
    return rows


def parse_float(s: str) -> float:
    try:
        return float(s)
    except Exception:
        return float('nan')


def extract_series(rows: List[Dict[str, str]]):
    sizes = []
    avg_send_mbits = []
    peak_send_mbits = []
    avg_send_ms = []
    peak_send_ms = []
    for r in rows:
        sizes.append(int(r.get(COLS['size'], '0')))
        avg_send_mbits.append(parse_float(r.get(COLS['avg_send_mbits'], 'nan')))
        peak_send_mbits.append(parse_float(r.get(COLS['peak_send_mbits'], 'nan')))
        avg_send_ms.append(parse_float(r.get(COLS['avg_send_ms'], 'nan')))
        peak_send_ms.append(parse_float(r.get(COLS['peak_send_ms'], 'nan')))
    return sizes, avg_send_mbits, peak_send_mbits, avg_send_ms, peak_send_ms


normal_rows = read_csv(NORMAL_CSV)
batch_rows = read_csv(BATCH_CSV)

n_sizes, n_avg_mbits, n_peak_mbits, n_avg_ms, n_peak_ms = extract_series(normal_rows)
b_sizes, b_avg_mbits, b_peak_mbits, b_avg_ms, b_peak_ms = extract_series(batch_rows)

# Desired X order and labels (equally spaced categories)
xticks = [
    1048576,        # 1MB
    10485760,       # 10MB
    104857600,      # 100MB
    524288000,      # 500MB
    1073741824,     # 1GB
    2147483648,     # 2GB
]
xtick_labels = ['1MB', '10MB', '100MB', '500MB', '1GB', '2GB']

# Build maps for fast lookup
def build_map(sizes, *series_lists):
    m = {}
    for i, sz in enumerate(sizes):
        m[int(sz)] = [lst[i] for lst in series_lists]
    return m

n_map = build_map(n_sizes, n_avg_mbits, n_peak_mbits, n_avg_ms, n_peak_ms)
b_map = build_map(b_sizes, b_avg_mbits, b_peak_mbits, b_avg_ms, b_peak_ms)

# Align series to desired order (missing entries become NaN)
import math
def align_series(m):
    out = [
        (m.get(sz, [math.nan, math.nan, math.nan, math.nan]))
        for sz in xticks
    ]
    # unzip lists into separate lists
    return [list(x) for x in zip(*out)]

n_avg_mbits, n_peak_mbits, n_avg_ms, n_peak_ms = align_series(n_map)
b_avg_mbits, b_peak_mbits, b_avg_ms, b_peak_ms = align_series(b_map)

# Positions for equally spaced categories
positions = list(range(len(xticks)))

# 1) Avg throughput comparison (Normal vs Batch)
plt.figure(figsize=(9, 5))
plt.plot(positions, n_avg_mbits, 'o-', label='Normal avg send (Mbits/s)')
plt.plot(positions, b_avg_mbits, 's-', label='Batch avg send (Mbits/s)')
plt.xticks(positions, xtick_labels, rotation=0)
plt.xlabel('Message size')
plt.ylabel('Throughput (Mbits/s)')
plt.title('Avg Send Throughput: Normal vs Batch')
plt.grid(True, which='both', linestyle=':')
plt.legend()
avg_thr_out = os.path.join(PLOTS_DIR, 'send_throughput_avg_comparison.png')
plt.tight_layout()
plt.savefig(avg_thr_out)
plt.close()

# 2) Peak throughput comparison (Normal vs Batch)
plt.figure(figsize=(9, 5))
plt.plot(positions, n_peak_mbits, 'o--', label='Normal peak send (Mbits/s)')
plt.plot(positions, b_peak_mbits, 's--', label='Batch peak send (Mbits/s)')
plt.xticks(positions, xtick_labels, rotation=0)
plt.xlabel('Message size')
plt.ylabel('Throughput (Mbits/s)')
plt.title('Peak Send Throughput: Normal vs Batch')
plt.grid(True, which='both', linestyle=':')
plt.legend()
peak_thr_out = os.path.join(PLOTS_DIR, 'send_throughput_peak_comparison.png')
plt.tight_layout()
plt.savefig(peak_thr_out)
plt.close()

# 3) Avg transfer time comparison (Normal vs Batch)
plt.figure(figsize=(9, 5))
plt.plot(positions, n_avg_ms, 'o-', label='Normal avg send time (ms)')
plt.plot(positions, b_avg_ms, 's-', label='Batch avg send time (ms)')
plt.xticks(positions, xtick_labels, rotation=0)
plt.xlabel('Message size')
plt.ylabel('Transfer time (ms)')
plt.title('Avg Send Transfer Time: Normal vs Batch')
plt.grid(True, which='both', linestyle=':')
plt.legend()
avg_time_out = os.path.join(PLOTS_DIR, 'send_transfer_time_avg_comparison.png')
plt.tight_layout()
plt.savefig(avg_time_out)
plt.close()

# 4) Peak transfer time comparison (Normal vs Batch)
plt.figure(figsize=(9, 5))
plt.plot(positions, n_peak_ms, 'o--', label='Normal peak send time (ms)')
plt.plot(positions, b_peak_ms, 's--', label='Batch peak send time (ms)')
plt.xticks(positions, xtick_labels, rotation=0)
plt.xlabel('Message size')
plt.ylabel('Transfer time (ms)')
plt.title('Peak Send Transfer Time: Normal vs Batch')
plt.grid(True, which='both', linestyle=':')
plt.legend()
peak_time_out = os.path.join(PLOTS_DIR, 'send_transfer_time_peak_comparison.png')
plt.tight_layout()
plt.savefig(peak_time_out)
plt.close()

print("Wrote plots:\n - {}\n - {}\n - {}\n - {}".format(avg_thr_out, peak_thr_out, avg_time_out, peak_time_out))

# Combined 4-in-1 figure (2x2 subplots)
fig, axes = plt.subplots(2, 2, figsize=(12, 8))

# (1) Avg throughput
ax = axes[0, 0]
ax.plot(positions, n_avg_mbits, 'o-', label='Normal avg (Mbits/s)')
ax.plot(positions, b_avg_mbits, 's-', label='Batch avg (Mbits/s)')
ax.set_xticks(positions)
ax.set_xticklabels(xtick_labels)
ax.set_xlabel('Message size')
ax.set_ylabel('Throughput (Mbits/s)')
ax.set_title('Avg Send Throughput')
ax.grid(True, which='both', linestyle=':')
ax.legend()

# (2) Peak throughput
ax = axes[0, 1]
ax.plot(positions, n_peak_mbits, 'o--', label='Normal peak (Mbits/s)')
ax.plot(positions, b_peak_mbits, 's--', label='Batch peak (Mbits/s)')
ax.set_xticks(positions)
ax.set_xticklabels(xtick_labels)
ax.set_xlabel('Message size')
ax.set_ylabel('Throughput (Mbits/s)')
ax.set_title('Peak Send Throughput')
ax.grid(True, which='both', linestyle=':')
ax.legend()

# (3) Avg transfer time
ax = axes[1, 0]
ax.plot(positions, n_avg_ms, 'o-', label='Normal avg (ms)')
ax.plot(positions, b_avg_ms, 's-', label='Batch avg (ms)')
ax.set_xticks(positions)
ax.set_xticklabels(xtick_labels)
ax.set_xlabel('Message size')
ax.set_ylabel('Transfer time (ms)')
ax.set_title('Avg Send Transfer Time')
ax.grid(True, which='both', linestyle=':')
ax.legend()

# (4) Peak transfer time
ax = axes[1, 1]
ax.plot(positions, n_peak_ms, 'o--', label='Normal peak (ms)')
ax.plot(positions, b_peak_ms, 's--', label='Batch peak (ms)')
ax.set_xticks(positions)
ax.set_xticklabels(xtick_labels)
ax.set_xlabel('Message size')
ax.set_ylabel('Transfer time (ms)')
ax.set_title('Peak Send Transfer Time')
ax.grid(True, which='both', linestyle=':')
ax.legend()

fig.tight_layout()
four_out = os.path.join(PLOTS_DIR, 'send_metrics_4in1.png')
fig.savefig(four_out)
plt.close(fig)

print(f"Also wrote combined figure:\n - {four_out}")
