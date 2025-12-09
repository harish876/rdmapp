#!/usr/bin/env python3
import csv
import os
from typing import List, Tuple
import matplotlib.pyplot as plt

BASE = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE, 'data')
PLOTS_DIR = os.path.join(BASE, 'plots')

NORMAL_CSV = os.path.join(DATA_DIR, 'rdma_normal.csv')
BATCH_CSV = os.path.join(DATA_DIR, 'rdma_message_batch.csv')

os.makedirs(PLOTS_DIR, exist_ok=True)

# Helpers

def read_normal(path: str) -> Tuple[List[str], List[float], List[float]]:
    sizes = []
    avg = []
    peak = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sizes.append(row['size(bytes)'])
            avg.append(float(row['avg_send_throughput(MB/s)']))
            peak.append(float(row['peak_send_throughput(MB/s)']))
    return sizes, avg, peak


def read_batch(path: str) -> Tuple[List[str], List[float], List[float]]:
    sizes = []
    avg = []
    peak = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sizes.append(row['size(bytes)'])
            avg.append(float(row['avg_send_throughput(MB/s)']))
            peak.append(float(row['peak_send_throughput(MB/s)']))
    return sizes, avg, peak


def align_series(xs1: List[str], ys1: List[float], xs2: List[str], ys2: List[float]):
    # Create a union of sizes preserving order as categorical labels
    order = []
    seen = set()
    for x in xs1 + xs2:
        if x not in seen:
            seen.add(x)
            order.append(x)
    m1 = {x: y for x, y in zip(xs1, ys1)}
    m2 = {x: y for x, y in zip(xs2, ys2)}
    y1 = [m1.get(x, 0.0) for x in order]
    y2 = [m2.get(x, 0.0) for x in order]
    return order, y1, y2


def plot_overlay(metric: str, normal_vals: List[float], batch_vals: List[float], labels: List[str], fname: str):
    plt.figure(figsize=(10, 5))
    x = range(len(labels))
    plt.plot(x, normal_vals, marker='o', label='Normal')
    plt.plot(x, batch_vals, marker='s', label='Message Batch')
    plt.xticks(x, labels, rotation=30)
    plt.ylabel(f'{metric} (MB/s)')
    plt.xlabel('Size (bytes)')
    plt.title(f'Overlay: {metric} vs Size')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend()
    out = os.path.join(PLOTS_DIR, fname)
    plt.tight_layout()
    plt.savefig(out)
    print(f'Wrote {out}')


def main():
    sizes_n, avg_n, peak_n = read_normal(NORMAL_CSV)
    sizes_b, avg_b, peak_b = read_batch(BATCH_CSV)

    labels, avg_n_aligned, avg_b_aligned = align_series(sizes_n, avg_n, sizes_b, avg_b)
    labels2, peak_n_aligned, peak_b_aligned = align_series(sizes_n, peak_n, sizes_b, peak_b)

    # Ensure labels match; otherwise use the union from the first alignment
    if labels != labels2:
        # Re-align peaks to labels from avg
        _, peak_n_aligned, peak_b_aligned = align_series(sizes_n, peak_n, sizes_b, peak_b)

    plot_overlay('Average Send Throughput', avg_n_aligned, avg_b_aligned, labels, 'overlay_avg_send_throughput.png')
    plot_overlay('Peak Send Throughput', peak_n_aligned, peak_b_aligned, labels, 'overlay_peak_send_throughput.png')

if __name__ == '__main__':
    main()
