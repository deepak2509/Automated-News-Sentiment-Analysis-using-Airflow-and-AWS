from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, glob
from collections import defaultdict

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"

def compute_bias(**context):
    os.makedirs(GOLD_PATH, exist_ok=True)
    files = glob.glob(os.path.join(SILVER_PATH, "sentiment_*.json"))
    if not files:
        return
    latest = max(files, key=os.path.getctime)
    with open(latest, "r") as f:
        data = json.load(f)

    groups = defaultdict(lambda: {"pos": 0, "total": 0})
    for rec in data:
        label = rec.get("pred_label", "").lower()
        group = rec.get("language", "unknown") if "language" in rec else "en"
        groups[group]["total"] += 1
        if label == "positive":
            groups[group]["pos"] += 1

    metrics = []
    rates = {g: v["pos"] / v["total"] if v["total"] > 0 else 0 for g, v in groups.items()}
    if rates:
        baseline = list(rates.values())[0]
        for g, r in rates.items():
            parity_gap = r - baseline
            disparate_impact = r / baseline if baseline > 0 else None
            metrics.append({
                "timestamp": datetime.utcnow().isoformat(),
                "group": g,
                "positive_rate": r,
                "parity_gap": parity_gap,
                "disparate_impact": disparate_impact,
            })

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(GOLD_PATH, f"bias_metrics_{ts}.json")
    with open(out_file, "w") as f:
        json.dump(metrics, f)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bias_metrics_dag",
    default_args=default_args,
    description="Compute bias metrics from sentiment results",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    bias_task = PythonOperator(
        task_id="compute_bias",
        python_callable=compute_bias,
        provide_context=True,
    )
    bias_task
