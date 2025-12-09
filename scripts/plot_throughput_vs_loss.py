#!/usr/bin/env python3
"""
Plot throughput vs loss% for different message sizes.

Input CSV columns (header row):
    size, loss, SNDR TP (Mbps), SDNR TT (ms), RCVR TP (Mbps), RCVR TT (ms)

Example rows (tab or comma delimited is fine as long as DictReader sees the names):
    1GiB,0%,197.5293,43506.47,174.944,49136.61
    1GiB,0.50%,96.43775714,89097.95714,89.98487143,95465.34286
    ...

Outputs PNGs into ../plots/ (paired subplots):
    - throughput_vs_loss.png        (a) Sender, (b) Receiver
    - transfer_time_vs_loss.png     (a) Sender, (b) Receiver
"""

import csv
import os
from typing import Dict, List, Tuple
import math
import matplotlib.pyplot as plt

# Adjust ROOT/DATA_DIR/PLOTS_DIR to match your repo layout
ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
PLOTS_DIR = os.path.join(ROOT, "plots")
LOSS_CSV = os.path.join(DATA_DIR, "SR_data.csv")  # <-- change filename if needed

os.makedirs(PLOTS_DIR, exist_ok=True)

# Column names in the CSV
COLS = {
    "size": "size",
    "loss": "loss",
    "sndr_tp": "SNDR TP (Mbps)",
    "sndr_tt": "SDNR TT (ms)",  # Note: typo in CSV header "SDNR" instead of "SNDR"
    "rcvr_tp": "RCVR TP (Mbps)",
    "rcvr_tt": "RCVR TT (ms)",
}


def parse_float(s: str) -> float:
    try:
        return float(s)
    except Exception:
        return float("nan")


def parse_loss_percent(s: str) -> float:
    """
    Convert strings like '0%', '0.50%' -> float percentage value (e.g., 0.0, 0.5).
    """
    if s is None:
        return float("nan")
    s = s.strip()
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return float("nan")


def read_rows(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # skip if no size or loss
            if not r.get(COLS["size"]) or not r.get(COLS["loss"]):
                continue
            rows.append(r)
    return rows


def build_series_by_size(
    rows: List[Dict[str, str]],
    value_col: str,
) -> Dict[str, List[Tuple[float, float]]]:
    """
    For a given value column (throughput or transfer time), group points by message size.

    Returns:
        { size_str: [(loss_percent, value), ...], ... }
    """
    grouped: Dict[str, List[Tuple[float, float]]] = {}
    for r in rows:
        size = r.get(COLS["size"], "").strip()
        loss_val = parse_loss_percent(r.get(COLS["loss"], ""))
        value = parse_float(r.get(value_col, "nan"))

        if math.isnan(loss_val) or math.isnan(value) or not size:
            continue

        grouped.setdefault(size, []).append((loss_val, value))

    # Sort each size's series by loss ascending
    for size, pts in grouped.items():
        pts.sort(key=lambda x: x[0])

    return grouped


def _plot_single_loss_axis(ax, series, title, ylabel):
    """Helper to draw one loss-based axis."""
    size_order = ["1MiB", "10MiB", "100MiB", "500MiB", "1GiB"]
    size_styles = {
        "1MiB": "o-",
        "10MiB": "s-",
        "100MiB": "^-",
        "500MiB": "d-",
        "1GiB": "x-",
    }

    for size in size_order:
        if size not in series:
            continue
        pts = series[size]
        losses = [p[0] for p in pts]
        vals = [p[1] for p in pts]
        style = size_styles.get(size, "o-")
        ax.plot(losses, vals, style, label=size)

    for size, pts in series.items():
        if size in size_order:
            continue
        losses = [p[0] for p in pts]
        vals = [p[1] for p in pts]
        ax.plot(losses, vals, "o-", label=size)

    loss_ticks = [0.0, 0.5, 1.0, 5.0, 10.0]
    loss_labels = ["0%", "0.5%", "1%", "5%", "10%"]
    ax.set_xticks(loss_ticks, loss_labels)
    ax.set_xlabel("Loss (%)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, which="both", linestyle=":")
    ax.legend(title="Message size")


def plot_pair_vs_loss(
    series_sndr: Dict[str, List[Tuple[float, float]]],
    series_rcvr: Dict[str, List[Tuple[float, float]]],
    title: str,
    ylabel: str,
    filename: str,
):
    """
    Draw sender and receiver subplots stacked vertically.
    """
    fig, axes = plt.subplots(2, 1, figsize=(7, 9), sharex=False, sharey=False)
    _plot_single_loss_axis(axes[0], series_sndr, "", ylabel)
    axes[0].text(0.02, 1.04, "(a) Sender", ha="left", va="bottom", transform=axes[0].transAxes)

    _plot_single_loss_axis(axes[1], series_rcvr, "", ylabel)
    axes[1].text(0.02, 1.04, "(b) Receiver", ha="left", va="bottom", transform=axes[1].transAxes)

    fig.suptitle(title)
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    out_path = os.path.join(PLOTS_DIR, filename)
    plt.savefig(out_path)
    plt.close(fig)
    print(f"Wrote {out_path}")


def main():
    rows = read_rows(LOSS_CSV)

    # Throughput plots (paired)
    sndr_tp_series = build_series_by_size(rows, COLS["sndr_tp"])
    rcvr_tp_series = build_series_by_size(rows, COLS["rcvr_tp"])
    plot_pair_vs_loss(
        sndr_tp_series,
        rcvr_tp_series,
        title="RDMA SR - Throughput vs Loss",
        ylabel="Throughput (Mbps)",
        filename="throughput_vs_loss.png",
    )

    # Transfer time plots (paired)
    sndr_tt_series = build_series_by_size(rows, COLS["sndr_tt"])
    rcvr_tt_series = build_series_by_size(rows, COLS["rcvr_tt"])
    plot_pair_vs_loss(
        sndr_tt_series,
        rcvr_tt_series,
        title="RDMA SR - Transfer Time vs Loss",
        ylabel="Transfer Time (ms)",
        filename="transfer_time_vs_loss.png",
    )


if __name__ == "__main__":
    main()
