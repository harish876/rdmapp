#!/usr/bin/env python3
import csv
import os
from pathlib import Path
import matplotlib.pyplot as plt

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
PLOTS_DIR = Path(__file__).resolve().parents[1] / 'plots'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

IB_CSV = DATA_DIR / 'ib_write_bw_uc.csv'
BATCH_CSV = DATA_DIR / 'rdma_message_batch.csv'

# Helper to read CSV safely

def normalize_size_bytes(b: int) -> int:
    """Snap sizes within 1MB of a GB boundary to exact GB to avoid duplicate labels.
    Also fixes known malformed 2GB entry (2147483647 -> 2147483648).
    """
    GB = 1024**3
    MB = 1024**2
    # Fix common malformed 2GB value
    if b == 2147483647:
        return 2 * GB
    # Snap near-GB sizes
    gb_rounded = round(b / GB)
    if abs(b - gb_rounded * GB) <= MB:
        return max(1, int(gb_rounded)) * GB
    return b


def read_ib_bw(path):
    sizes = []
    avg = []
    peak = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                size_raw = int(row['bytes'])
                size = normalize_size_bytes(size_raw)
            except Exception:
                # skip malformed rows
                continue
            sizes.append(size)
            # IB CSV columns: avg_MBps, peak_MBps
            avg.append(float(row.get('avg_MBps', 0) or 0))
            peak.append(float(row.get('peak_MBps', 0) or 0))
    return sizes, avg, peak


def read_batch(path):
    sizes = []
    avg = []
    peak = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            size_raw = int(row['size(bytes)'])
            size = normalize_size_bytes(size_raw)
            sizes.append(size)
            avg.append(float(row['avg_send_throughput(MB/s)']))
            peak.append(float(row['peak_send_throughput(MB/s)']))
    return sizes, avg, peak


def make_labels(sizes):
    # Convert bytes to human-readable labels; round near exact GB boundaries
    labels = []
    GB = 1024**3
    MB = 1024**2
    for b in sizes:
        # If within 1MB of an exact GB, snap to GB label
        if abs(b - round(b / GB) * GB) <= MB:
            gb = max(1, int(round(b / GB)))
            labels.append(f"{gb}GB")
        else:
            labels.append(f"{int(round(b / MB))}MB")
    return labels


def plot_overlay(x_labels, ib_avg, ib_peak, batch_avg, batch_peak, title_suffix=''):
    plt.figure(figsize=(10, 6))
    x = list(range(len(x_labels)))

    # Avg throughput overlay
    plt.plot(x, ib_avg, marker='o', label='ib_write_bw avg (MB/s)')
    plt.plot(x, batch_avg, marker='o', label='RDMA message batch avg (MB/s)')
    plt.xticks(x, x_labels, rotation=0)
    plt.xlabel('Message size')
    plt.ylabel('Average throughput (MB/s)')
    plt.title(f'Average Throughput Overlay {title_suffix}')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend()
    avg_path = PLOTS_DIR / f'overlay_avg_ib_vs_batch{title_suffix.replace(" ", "_")}.png'
    plt.tight_layout()
    plt.savefig(avg_path)
    plt.close()

    # Peak throughput overlay
    plt.figure(figsize=(10, 6))
    plt.plot(x, ib_peak, marker='o', label='ib_write_bw peak (MB/s)')
    plt.plot(x, batch_peak, marker='o', label='RDMA message batch peak (MB/s)')
    plt.xticks(x, x_labels, rotation=0)
    plt.xlabel('Message size')
    plt.ylabel('Peak throughput (MB/s)')
    plt.title(f'Peak Throughput Overlay {title_suffix}')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend()
    peak_path = PLOTS_DIR / f'overlay_peak_ib_vs_batch{title_suffix.replace(" ", "_")}.png'
    plt.tight_layout()
    plt.savefig(peak_path)
    plt.close()

    return avg_path, peak_path


def main():
    ib_sizes, ib_avg, ib_peak = read_ib_bw(IB_CSV)
    batch_sizes, batch_avg, batch_peak = read_batch(BATCH_CSV)

    # Align on the UNION of sizes; if a size is missing in IB or batch,
    # treat its throughput as 0 to keep the overlay complete.
    common = sorted(set(ib_sizes) | set(batch_sizes))

    # Reindex values by size
    ib_avg_map = {s: v for s, v in zip(ib_sizes, ib_avg)}
    ib_peak_map = {s: v for s, v in zip(ib_sizes, ib_peak)}
    batch_avg_map = {s: v for s, v in zip(batch_sizes, batch_avg)}
    batch_peak_map = {s: v for s, v in zip(batch_sizes, batch_peak)}

    ib_avg_aligned = [ib_avg_map.get(s, 0.0) for s in common]
    ib_peak_aligned = [ib_peak_map.get(s, 0.0) for s in common]
    batch_avg_aligned = [batch_avg_map.get(s, 0.0) for s in common]
    batch_peak_aligned = [batch_peak_map.get(s, 0.0) for s in common]

    labels = make_labels(common)
    avg_path, peak_path = plot_overlay(labels, ib_avg_aligned, ib_peak_aligned,
                                       batch_avg_aligned, batch_peak_aligned)
    print(f"Wrote {avg_path}\nWrote {peak_path}")

if __name__ == '__main__':
    main()
