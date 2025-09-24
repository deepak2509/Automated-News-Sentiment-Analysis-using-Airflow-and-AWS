from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Wrapper functions (imports inside) ---

def extract_news_fn():
    from newsapi_client import fetch_and_store_news
    fetch_and_store_news()

def run_sentiment_fn():
    from to_silver import bronze_to_silver
    bronze_to_silver()

def bias_metrics_fn():
    from fairness_metrics import compute_fairness_metrics
    compute_fairness_metrics()

def drift_metrics_fn():
    from drift_metrics import compute_drift_metrics
    compute_drift_metrics()

def load_data_fn():
    from to_warehouse import load_to_warehouse
    load_to_warehouse()

def upload_s3_fn():
    from s3_utils import upload_to_s3
    import glob, os
    from datetime import datetime

    dirs = {
        "bronze": "/opt/airflow/data/bronze",
        "silver": "/opt/airflow/data/silver",
        "gold": "/opt/airflow/data/gold"
    }

    for layer, path in dirs.items():
        files = glob.glob(os.path.join(path, "*.json"))
        if not files:
            print(f"No {layer} output found to upload.")
            continue

        for f in files:
            fname = os.path.basename(f)
            s3_key = f"{layer}/{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{fname}"
            upload_to_s3(f, s3_key)

# --- DAG definition ---
with DAG(
    dag_id="end_to_end_pipeline_dag",
    default_args=default_args,
    description="End-to-end pipeline from news → sentiment → metrics → warehouse → S3",
    schedule_interval="@hourly",
    start_date=datetime(2025, 9, 18, 13, 0, 0),
    catchup=False,
) as dag:

    extract_news = PythonOperator(
        task_id="extract_news",
        python_callable=extract_news_fn,
    )

    run_sentiment = PythonOperator(
        task_id="run_sentiment",
        python_callable=run_sentiment_fn,
    )

    bias_metrics = PythonOperator(
        task_id="bias_metrics",
        python_callable=bias_metrics_fn,
    )

    drift_metrics = PythonOperator(
        task_id="drift_metrics",
        python_callable=drift_metrics_fn,
    )

    load_data = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_data_fn,
    )

    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_s3_fn,
    )

    # Dependencies
    extract_news >> run_sentiment >> [bias_metrics, drift_metrics] >> load_data >> upload_s3
