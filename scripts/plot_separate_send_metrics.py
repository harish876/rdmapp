#!/usr/bin/env python3
import csv
import os
from typing import List, Dict
import matplotlib.pyplot as plt

BASE = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE, 'data')
PLOTS_DIR = os.path.join(BASE, 'plots')
NORMAL_CSV = os.path.join(DATA_DIR, "rdma_normal.csv")
BATCH_CSV = os.path.join(DATA_DIR, "rdma_message_batch.csv")

FIELDS = {
    "size": "size(bytes)",
    "avg_send_mb_s": "avg_send_throughput(MB/s)",
    "avg_send_mbits_s": "avg_send_throughput(MBits/s)",
    "avg_send_ms": "avg_send_transfer_time(ms)",
    "peak_send_mb_s": "peak_send_throughput(MB/s)",
    "peak_send_mbits_s": "peak_send_throughput(MBits/s)",
    "peak_send_ms": "peak_send_transfer_time(ms)",
}


def read_rows(path: str) -> List[Dict[str, str]]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def parse_sizes(rows: List[Dict[str, str]]) -> List[str]:
    # Convert sizes to human-friendly labels
    labels = []
    for r in rows:
        sz = int(r[FIELDS["size"]])
        if sz % (1024 ** 3) == 0:
            labels.append(f"{sz // (1024 ** 3)}GB")
        else:
            labels.append(f"{sz // (1024 ** 2)}MB")
    return labels


def get_values(rows: List[Dict[str, str]], field: str) -> List[float]:
    return [float(r[field]) for r in rows]


def plot_send_metrics(rows: List[Dict[str, str]], title: str, out_path: str):
    labels = parse_sizes(rows)
    x = range(len(labels))

    avg_mb = get_values(rows, FIELDS["avg_send_mb_s"])
    peak_mb = get_values(rows, FIELDS["peak_send_mb_s"])
    avg_ms = get_values(rows, FIELDS["avg_send_ms"])
    peak_ms = get_values(rows, FIELDS["peak_send_ms"])

    fig, axs = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle(title)

    # Avg throughput (MB/s)
    axs[0, 0].plot(x, avg_mb, marker="o", label="avg send MB/s")
    axs[0, 0].set_title("Avg Send Throughput (MB/s)")
    axs[0, 0].set_xticks(x)
    axs[0, 0].set_xticklabels(labels, rotation=0)
    axs[0, 0].set_ylabel("MB/s")
    axs[0, 0].grid(True, linestyle=":", alpha=0.5)

    # Peak throughput (MB/s)
    axs[0, 1].plot(x, peak_mb, marker="o", color="tab:orange", label="peak send MB/s")
    axs[0, 1].set_title("Peak Send Throughput (MB/s)")
    axs[0, 1].set_xticks(x)
    axs[0, 1].set_xticklabels(labels, rotation=0)
    axs[0, 1].set_ylabel("MB/s")
    axs[0, 1].grid(True, linestyle=":", alpha=0.5)

    # Avg transfer time (ms)
    axs[1, 0].plot(x, avg_ms, marker="o", color="tab:green", label="avg send ms")
    axs[1, 0].set_title("Avg Send Transfer Time (ms)")
    axs[1, 0].set_xticks(x)
    axs[1, 0].set_xticklabels(labels, rotation=0)
    axs[1, 0].set_ylabel("ms")
    axs[1, 0].grid(True, linestyle=":", alpha=0.5)

    # Peak transfer time (ms)
    axs[1, 1].plot(x, peak_ms, marker="o", color="tab:red", label="peak send ms")
    axs[1, 1].set_title("Peak Send Transfer Time (ms)")
    axs[1, 1].set_xticks(x)
    axs[1, 1].set_xticklabels(labels, rotation=0)
    axs[1, 1].set_ylabel("ms")
    axs[1, 1].grid(True, linestyle=":", alpha=0.5)

    for ax in axs.flat:
        ax.legend()

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(out_path, dpi=120)
    print(f"Saved: {out_path}")


def main():
    normal_rows = read_rows(NORMAL_CSV)
    batch_rows = read_rows(BATCH_CSV)

    plot_send_metrics(normal_rows, "RDMA Normal - Send Metrics", os.path.join(PLOTS_DIR, "rdma_normal_send_metrics.png"))
    plot_send_metrics(batch_rows, "RDMA Message Batch - Send Metrics", os.path.join(PLOTS_DIR, "rdma_message_batch_send_metrics.png"))


if __name__ == "__main__":
    main()
