from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, glob
from collections import Counter
import math

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

def compute_drift(**context):
    os.makedirs(GOLD_PATH, exist_ok=True)
    files = glob.glob(os.path.join(SILVER_PATH, "sentiment_*.json"))
    if not files:
        return
    latest = max(files, key=os.path.getctime)
    with open(latest, "r") as f:
        data = json.load(f)

    labels = [rec.get("pred_label", "").lower() for rec in data if rec.get("pred_label")]
    total = len(labels)
    if total == 0:
        return
    current_dist = {k: v / total for k, v in Counter(labels).items()}

    if not os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "w") as f:
            json.dump(current_dist, f)
        return

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

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="drift_metrics_dag",
    default_args=default_args,
    description="Compute drift metrics from sentiment results",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    drift_task = PythonOperator(
        task_id="compute_drift",
        python_callable=compute_drift,
        provide_context=True,
    )
    drift_task
