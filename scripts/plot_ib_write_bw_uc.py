#!/usr/bin/env python3
"""
Plot ib_write_bw UC results from data/ib_write_bw_uc.csv.
- X axis: 1MB, 10MB, 100MB, 500MB, 1GB, 2GB (equally spaced)
- Lines: Peak BW (MB/s), Avg BW (MB/s), MsgRate (Mpps)
- If a size is missing (e.g., 2GB), default metrics to 0.
Outputs PNGs into ../plots/
"""
import csv
import os
from typing import Dict, List
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, 'data')
PLOTS_DIR = os.path.join(ROOT, 'plots')
CSV_PATH = os.path.join(DATA_DIR, 'ib_write_bw_uc.csv')

os.makedirs(PLOTS_DIR, exist_ok=True)

# Desired sizes and labels
SIZES = [
    1048576,        # 1MB
    10485760,       # 10MB
    104857600,      # 100MB
    524288000,      # 500MB
    1073741824,     # 1GB
    2147483648,     # 2GB
]
LABELS = ['1MB', '10MB', '100MB', '500MB', '1GB', '2GB']
POSITIONS = list(range(len(SIZES)))

# Read data
rows: List[Dict[str, str]] = []
with open(CSV_PATH, 'r') as f:
    reader = csv.DictReader(f)
    for r in reader:
        rows.append(r)

# Build map size -> metrics
m: Dict[int, Dict[str, float]] = {}
for r in rows:
    try:
        sz = int(r['bytes'])
    except Exception:
        continue
    m[sz] = {
        'peak_MBps': float(r.get('peak_MBps', '0') or 0),
        'avg_MBps': float(r.get('avg_MBps', '0') or 0),
        'msg_rate_Mpps': float(r.get('msg_rate_Mpps', '0') or 0),
    }

# Align to desired sizes, default to 0 if missing
peak = [m.get(sz, {'peak_MBps': 0})['peak_MBps'] for sz in SIZES]
avg = [m.get(sz, {'avg_MBps': 0})['avg_MBps'] for sz in SIZES]
rate = [m.get(sz, {'msg_rate_Mpps': 0})['msg_rate_Mpps'] for sz in SIZES]

# Plot BW (MB/s)
plt.figure(figsize=(9, 5))
plt.plot(POSITIONS, peak, 'o--', label='Peak BW (MB/s)')
plt.plot(POSITIONS, avg, 's-', label='Avg BW (MB/s)')
plt.xticks(POSITIONS, LABELS)
plt.xlabel('Message size')
plt.ylabel('Bandwidth (MB/s)')
plt.title('ib_write_bw (UC) Bandwidth vs Size')
plt.grid(True, which='both', linestyle=':')
plt.legend()
out_bw = os.path.join(PLOTS_DIR, 'ib_write_bw_uc_bandwidth.png')
plt.tight_layout()
plt.savefig(out_bw)
plt.close()

# Plot MsgRate (Mpps)
plt.figure(figsize=(9, 5))
plt.plot(POSITIONS, rate, 'd-', color='tab:purple', label='MsgRate (Mpps)')
plt.xticks(POSITIONS, LABELS)
plt.xlabel('Message size')
plt.ylabel('Message Rate (Mpps)')
plt.title('ib_write_bw (UC) Message Rate vs Size')
plt.grid(True, which='both', linestyle=':')
plt.legend()
out_rate = os.path.join(PLOTS_DIR, 'ib_write_bw_uc_msgrate.png')
plt.tight_layout()
plt.savefig(out_rate)
plt.close()

print(f"Wrote plots:\n - {out_bw}\n - {out_rate}")
