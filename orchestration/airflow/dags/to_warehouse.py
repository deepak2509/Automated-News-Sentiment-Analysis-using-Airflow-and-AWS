import os
import json
import glob
import psycopg2
from s3_utils import upload_to_s3  

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")
DB_PORT = os.getenv("DB_PORT", "5432")

def load_to_warehouse():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )
    cur = conn.cursor()

    # Ensure tables exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_predictions (
            ts TIMESTAMP,
            source TEXT,
            title TEXT,
            pred_label TEXT,
            pred_score FLOAT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bias_metrics (
            ts TIMESTAMP,
            group_name TEXT,
            positive_rate FLOAT,
            parity_gap FLOAT,
            disparate_impact FLOAT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drift_metrics (
            ts TIMESTAMP,
            psi FLOAT,
            output_shift FLOAT,
            current_distribution JSONB,
            baseline_distribution JSONB
        );
    """)

    # --- Sentiment Predictions (Silver) ---
    files = glob.glob(os.path.join(SILVER_PATH, "sentiment_*.json"))
    if files:
        latest = max(files, key=os.path.getctime)
        with open(latest, "r") as f:
            data = json.load(f)
        for rec in data:
            cur.execute(
                "INSERT INTO sentiment_predictions (ts, source, title, pred_label, pred_score) VALUES (%s, %s, %s, %s, %s)",
                (rec.get("ts") or rec.get("processed_at"), 
                 rec.get("source", {}).get("name") if isinstance(rec.get("source"), dict) else rec.get("source"),
                 rec.get("title"), 
                 rec.get("pred_label"), 
                 rec.get("pred_score"))
            )

        #  Upload to S3
        upload_to_s3(latest, f"silver/{os.path.basename(latest)}")

    # --- Bias Metrics (Gold) ---
    bias_files = glob.glob(os.path.join(GOLD_PATH, "bias_metrics_*.json"))
    if bias_files:
        latest = max(bias_files, key=os.path.getctime)
        with open(latest, "r") as f:
            data = json.load(f)
        for rec in data:
            cur.execute(
                "INSERT INTO bias_metrics (ts, group_name, positive_rate, parity_gap, disparate_impact) VALUES (%s, %s, %s, %s, %s)",
                (rec["timestamp"], rec["group"], rec["positive_rate"], rec["parity_gap"], rec["disparate_impact"])
            )

        # Upload to S3
        upload_to_s3(latest, f"gold/{os.path.basename(latest)}")

    # --- Drift Metrics (Gold) ---
    drift_files = glob.glob(os.path.join(GOLD_PATH, "drift_metrics_*.json"))
    if drift_files:
        latest = max(drift_files, key=os.path.getctime)
        with open(latest, "r") as f:
            rec = json.load(f)
        cur.execute(
            "INSERT INTO drift_metrics (ts, psi, output_shift, current_distribution, baseline_distribution) VALUES (%s, %s, %s, %s, %s)",
            (rec["timestamp"], rec["psi"], rec["output_shift"], 
             json.dumps(rec["current_distribution"]), 
             json.dumps(rec["baseline_distribution"]))
        )

        #  Upload to S3
        upload_to_s3(latest, f"gold/{os.path.basename(latest)}")

    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded into Postgres and uploaded to S3.")


if __name__ == "__main__":
    load_to_warehouse()
