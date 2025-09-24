import os
import json
import glob
import math
from datetime import datetime
from collections import Counter

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"
BASELINE_FILE = os.path.join(GOLD_PATH, "baseline_distribution.json")

def calculate_psi(expected, actual):
    psi = 0.0
    for k in expected.keys():
        e = expected.get(k, 0.0001)
        a = actual.get(k, 0.0001)
        if e > 0 and a > 0:
            psi += (a - e) * math.log(a / e)
    return psi

def compute_drift_metrics():
    os.makedirs(GOLD_PATH, exist_ok=True)

    files = glob.glob(os.path.join(SILVER_PATH, "sentiment_*.json"))
    if not files:
        return None
    latest = max(files, key=os.path.getctime)

    with open(latest, "r") as f:
        data = json.load(f)

    labels = [rec.get("pred_label", "").lower() for rec in data if rec.get("pred_label")]
    total = len(labels)
    if total == 0:
        return None

    current_dist = {k: v / total for k, v in Counter(labels).items()}

    # If no baseline exists → save current as baseline
    if not os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "w") as f:
            json.dump(current_dist, f)
        return None

    with open(BASELINE_FILE, "r") as f:
        baseline_dist = json.load(f)

    psi = calculate_psi(baseline_dist, current_dist)

    pos_rate_baseline = baseline_dist.get("positive", 0)
    pos_rate_current = current_dist.get("positive", 0)
    output_shift = pos_rate_current - pos_rate_baseline

    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "current_distribution": current_dist,
        "baseline_distribution": baseline_dist,
        "psi": psi,
        "output_shift": output_shift,
    }

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(GOLD_PATH, f"drift_metrics_{ts}.json")
    with open(out_file, "w") as f:
        json.dump(metrics, f)

    return out_file

if __name__ == "__main__":
    file_path = compute_drift_metrics()
    if file_path:
        print(f"Drift metrics saved: {file_path}")
    else:
        print("No drift metrics computed (baseline initialized or no data).")
