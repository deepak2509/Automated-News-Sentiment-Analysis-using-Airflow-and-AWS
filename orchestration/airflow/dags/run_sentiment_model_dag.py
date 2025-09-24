from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transformers import pipeline
import json
import glob

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

def run_sentiment_analysis():
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english"
    )

    files = glob.glob(f"{BRONZE_PATH}/news_*.json")
    if not files:
        print("No news files found.")
        return

    latest = max(files, key=lambda f: f)
    with open(latest, "r") as f:
        articles = json.load(f)

    results = []
    for art in articles:
        text = art.get("title", "") + " " + art.get("description", "")
        if text.strip():
            result = sentiment_pipeline(text[:512])[0]
            art["sentiment"] = result
        results.append(art)

    out_file = f"{SILVER_PATH}/sentiment_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    with open(out_file, "w") as f:
        json.dump(results, f)

    print(f"Saved sentiment results to {out_file}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "run_sentiment_model_dag",
    default_args=default_args,
    description="Run Hugging Face sentiment analysis",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    run_sentiment = PythonOperator(
        task_id="run_sentiment",
        python_callable=run_sentiment_analysis,
    )

    run_sentiment
