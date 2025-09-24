import os
import json
import glob
from datetime import datetime
from transformers import pipeline
from s3_utils import upload_to_s3  

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

# Load HuggingFace sentiment model
sentiment_model = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english"
)

def bronze_to_silver():
    os.makedirs(SILVER_PATH, exist_ok=True)

    # Find the most recent bronze file
    files = glob.glob(os.path.join(BRONZE_PATH, "news_*.json"))
    if not files:
        return None
    latest = max(files, key=os.path.getctime)

    # Load bronze records
    with open(latest, "r") as f:
        articles = json.load(f)

    results = []
    for rec in articles:
        # Handle dict vs string safely
        if isinstance(rec, dict):
            text = rec.get("title") or rec.get("description") or ""
        elif isinstance(rec, str):
            text = rec
        else:
            text = ""

        if not text.strip():
            continue

        # Run sentiment analysis (truncate to 512 chars for safety)
        pred = sentiment_model(text[:512])[0]

        enriched = {
            "text": text,
            "pred_label": pred["label"],
            "pred_score": float(pred["score"]),
            "processed_at": datetime.utcnow().isoformat()
        }

        # Merge enriched data
        if isinstance(rec, dict):
            rec.update(enriched)
            results.append(rec)
        else:
            results.append(enriched)

    # Save silver file locally
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(SILVER_PATH, f"sentiment_{ts}.json")

    with open(out_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # ✅ Save also to S3
    s3_key = f"silver/{os.path.basename(out_file)}"
    upload_to_s3(out_file, s3_key)

    print(f"✅ Silver file saved locally and uploaded to S3: {s3_key}")
    return out_file


if __name__ == "__main__":
    file_path = bronze_to_silver()
    if file_path:
        print(f"Silver file saved: {file_path}")
    else:
        print("No bronze files found to process.")
