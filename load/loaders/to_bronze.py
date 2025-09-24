import os
import json
import uuid
from datetime import datetime
from extract.schemas.news_event import NewsEvent, NewsSource
from extract.utils.lang_detect import detect_language
from orchestration.airflow.dags.s3_utils import upload_to_s3  

BRONZE_PATH = "/opt/airflow/data/bronze"

def save_to_bronze(raw_data: dict) -> str:
    os.makedirs(BRONZE_PATH, exist_ok=True)

    events = []
    for article in raw_data.get("articles", []):
        try:
            lang = detect_language(article.get("title", ""))

            event = NewsEvent(
                event_id=str(uuid.uuid4()),
                ts=datetime.utcnow(),
                source=NewsSource(**article.get("source", {})),
                author=article.get("author"),
                title=article.get("title") or "",
                description=article.get("description"),
                url=article.get("url"),
                published_at=article.get("publishedAt"),
                language=lang,
                region=None
            )
            events.append(event.dict())
        except Exception as e:
            print(f"Skipping invalid article: {e}")

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    out_file = os.path.join(BRONZE_PATH, f"news_{ts}.json")

    # Save locally
    with open(out_file, "w") as f:
        json.dump(events, f, default=str)

    # Save to S3
    s3_key = f"bronze/{os.path.basename(out_file)}"
    upload_to_s3(out_file, s3_key)

    print(f" Bronze file saved locally and uploaded to S3: {s3_key}")
    return out_file


if __name__ == "__main__":
    sample = {
        "articles": [
            {"source": {"id": "cnn", "name": "CNN"}, "title": "Markets fall sharply today"},
            {"source": {"id": None, "name": "BBC"}, "title": "La economía mundial enfrenta desafíos"}
        ]
    }
    file_path = save_to_bronze(sample)
    print(f"Bronze file saved: {file_path}")
