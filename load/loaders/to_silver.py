import os
import json
import glob
from datetime import datetime
from transformers import pipeline

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

sentiment_model = pipeline("sentiment-analysis")

def bronze_to_silver():
    os.makedirs(SILVER_PATH, exist_ok=True)
    files = glob.glob(os.path.join(BRONZE_PATH, "news_*.json"))
    if not files:
        return None
    latest = max(files, key=os.path.getctime)

    with open(latest, "r") as f:
        articles = json.load(f)

    results = []
    for rec in articles:
        text = rec.get("title", "")
        if not text.strip():
            continue
        pred = sentiment_model(text[:512])[0]
        rec.update({
            "pred_label": pred["label"],
            "pred_score": float(pred["score"]),
            "processed_at": datetime.utcnow().isoformat()
        })
        results.append(rec)

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(SILVER_PATH, f"sentiment_{ts}.json")

    with open(out_file, "w") as f:
        json.dump(results, f, default=str)

    return out_file

if __name__ == "__main__":
    file_path = bronze_to_silver()
    if file_path:
        print(f"Silver file saved: {file_path}")
    else:
        print("No bronze files found to process.")
