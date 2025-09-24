from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, os, json

BRONZE_PATH = "/opt/airflow/data/bronze"

def fetch_news(**context):
    url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=10&apiKey=8eff43b0196d4bcc9972a7c920e9c037"
    resp = requests.get(url).json()
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    os.makedirs(BRONZE_PATH, exist_ok=True)
    out_file = os.path.join(BRONZE_PATH, f"news_{ts}.json")
    with open(out_file, "w") as f:
        json.dump(resp, f)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_news_dag",
    default_args=default_args,
    description="Extract news from NewsAPI",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_news,
        provide_context=True,
    )
    extract_task
