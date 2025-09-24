# Automated News Sentiment Analytics Pipeline

##  Project Overview  
This project demonstrates an **end-to-end data engineering and analytics pipeline** that ingests news articles from an API, stores them in **Amazon S3**, processes and cleans the data using **AWS Glue**, queries the curated data with **Amazon Athena**, and visualizes insights in **Power BI**.  

The main goal is to extract **meaningful insights** about news sentiment (positive vs negative) over time, across sources, authors, and topics.  

---

##  Architecture  

1. **Data Ingestion (Bronze Layer)**  
   - News articles ingested from an API.  
   - Stored as **raw JSON** files in Amazon S3 (`bronze`).  

2. **Data Transformation (Silver Layer)**  
   - AWS Glue ETL jobs clean and flatten JSON (e.g., nested `source` fields).  
   - Add sentiment labels (`pred_label`, `pred_score`) and processing timestamp.  
   - Data written to S3 in **Parquet format** (`silver_cleaned`).  

3. **Query Layer (Athena)**  
   - External tables defined over Parquet data in S3.  
   - SQL queries used to explore sentiment, confidence, and article metadata.  

4. **Visualization Layer (Power BI)**  
   - Power BI connects to Athena.  
   - Dashboards provide **trends, bias detection, author/source analysis, and topic-based insights**.  

---

## 🗄️ Data Schema  

**Final Silver Table (`silver_news_final`)**

| Column        | Type    | Description |
|---------------|---------|-------------|
| `source_id`   | string  | Source identifier |
| `source_name` | string  | News source name |
| `author`      | string  | Article author |
| `title`       | string  | Article headline |
| `description` | string  | Short description |
| `url`         | string  | Article URL |
| `urlToImage`  | string  | Image link |
| `publishedat` | string  | Publication timestamp (ISO 8601) |
| `content`     | string  | Main article content |
| `pred_label`  | string  | Sentiment (POSITIVE/NEGATIVE) |
| `pred_score`  | double  | Confidence score (0–1) |
| `processed_at`| string  | ETL processing timestamp |
| `region`      | string  | (Optional) Region info |

---

## 📊 Business Questions & SQL Queries  
### 1. How does overall news sentiment change over time?  
```sql
SELECT 
    date(from_iso8601_timestamp(publishedat)) AS pub_date,
    pred_label,
    COUNT(*) AS article_count,
    AVG(pred_score) AS avg_confidence
FROM silver_news_final
WHERE publishedat IS NOT NULL
GROUP BY date(from_iso8601_timestamp(publishedat)), pred_label
ORDER BY pub_date, pred_label;

```
### 2.Which news sources consistently produce more positive vs negative coverage?
```sql
SELECT 
    source_name,
    pred_label,
    COUNT(*) AS article_count
FROM elt_project.silver_news_final
WHERE source_name IS NOT NULL
GROUP BY source_name, pred_label
ORDER BY article_count DESC;
```

### 3. What percentage of articles are positive vs negative overall?
```sql
SELECT 
    pred_label,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM newspipeline.silver_news_final) AS percentage
FROM elt_project.silver_news_final
GROUP BY pred_label;
```
### 4. How does sentiment differ across specific topics (e.g., elections, economy, sports)?
```sql
SELECT topic, pred_label, COUNT(*) AS article_count
FROM (
    SELECT 
        CASE 
            WHEN LOWER(title) LIKE '%election%' OR LOWER(content) LIKE '%election%' THEN 'Elections'
            WHEN LOWER(title) LIKE '%economy%' OR LOWER(content) LIKE '%economy%' THEN 'Economy'
            WHEN LOWER(title) LIKE '%sport%'   OR LOWER(content) LIKE '%sport%'   THEN 'Sports'
            ELSE 'Other'
        END AS topic,
        pred_label
    FROM elt_project.silver_news_final
) t
GROUP BY topic, pred_label
ORDER BY topic, article_count DESC;
```

### 5. Which journalists/authors contribute the most positive or negative articles?
```sql
SELECT 
    author,
    pred_label,
    COUNT(*) AS article_count
FROM elt_project.silver_news_final
WHERE author IS NOT NULL
GROUP BY author, pred_label
ORDER BY article_count DESC
LIMIT 20;
```


