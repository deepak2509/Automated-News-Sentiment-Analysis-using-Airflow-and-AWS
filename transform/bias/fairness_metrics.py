import os
import json
import glob
from datetime import datetime
from collections import defaultdict

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"

def compute_fairness_metrics():
    os.makedirs(GOLD_PATH, exist_ok=True)

    files = glob.glob(os.path.join(SILVER_PATH, "sentiment_*.json"))
    if not files:
        return None
    latest = max(files, key=os.path.getctime)

    with open(latest, "r") as f:
        data = json.load(f)

    groups = defaultdict(lambda: {"pos": 0, "total": 0})
    for rec in data:
        label = rec.get("pred_label", "").lower()
        group = rec.get("language", "unknown")
        groups[group]["total"] += 1
        if label == "positive":
            groups[group]["pos"] += 1

    rates = {g: v["pos"] / v["total"] if v["total"] > 0 else 0 for g, v in groups.items()}
    baseline_group = list(rates.keys())[0] if rates else None

    metrics = []
    if baseline_group:
        baseline_rate = rates[baseline_group]
        for g, r in rates.items():
            metrics.append({
                "timestamp": datetime.utcnow().isoformat(),
                "group": g,
                "positive_rate": r,
                "parity_gap": r - baseline_rate,
                "disparate_impact": (r / baseline_rate) if baseline_rate > 0 else None,
            })

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(GOLD_PATH, f"fairness_metrics_{ts}.json")
    with open(out_file, "w") as f:
        json.dump(metrics, f)

    return out_file

if __name__ == "__main__":
    file_path = compute_fairness_metrics()
    if file_path:
        print(f"Fairness metrics saved: {file_path}")
    else:
        print("No sentiment predictions found.")
