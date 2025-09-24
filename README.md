📰 Automated News Sentiment Analytics Pipeline
📌 Project Overview

This project demonstrates an end-to-end data engineering and analytics pipeline that ingests news articles from an API, stores them in Amazon S3, processes and cleans the data using AWS Glue, queries the curated data with Amazon Athena, and visualizes business insights in Power BI.

The goal is to extract meaningful insights about news sentiment (positive vs negative) over time, across sources, authors, and topics.

⚙️ Architecture

Data Ingestion (Bronze Layer)

News articles ingested from external APIs.

Stored as raw JSON files in Amazon S3 (bronze bucket).

Data Transformation (Silver Layer)

AWS Glue ETL jobs clean and flatten JSON (e.g., source fields, nested attributes).

Adds metadata like processing timestamp, sentiment label, and score.

Stores structured data in Parquet format in S3 (silver_cleaned bucket).

Query Layer (Athena)

External tables created in Athena for silver_news_final.

SQL queries used to aggregate, analyze, and prepare data for visualization.

Visualization Layer (Power BI)

Power BI connects to Athena using the ODBC driver.

Dashboards show sentiment trends, source bias, author contributions, topic insights, and confidence distributions.

🗄️ Data Schema

Final Silver Table (silver_news_final)

Column	Type	Description
source_id	string	Source identifier
source_name	string	News source name
author	string	Article author
title	string	Article headline
description	string	Short description
url	string	Article URL
urlToImage	string	Image link
publishedat	string	Publication timestamp (ISO 8601)
content	string	Main article content
pred_label	string	Sentiment (POSITIVE/NEGATIVE)
pred_score	double	Confidence score (0–1)
processed_at	string	ETL processing timestamp
region	string	(Optional) Region info
📊 Business Questions & Insights
1. How does overall news sentiment (positive vs negative) change over time (hourly/daily)?
SELECT 
    date(from_iso8601_timestamp(publishedAt)) AS pub_date,
    pred_label,
    COUNT(*) AS article_count,
    AVG(pred_score) AS avg_confidence
FROM silver_news_final
WHERE publishedAt IS NOT NULL
GROUP BY date(from_iso8601_timestamp(publishedAt)), pred_label
ORDER BY pub_date, pred_label;


Visualization: Line chart (Date vs Articles, split by sentiment).

2. Which news sources consistently produce more positive vs negative coverage?
SELECT 
    source_name,
    pred_label,
    COUNT(*) AS article_count
FROM silver_news_final
WHERE source_name IS NOT NULL
GROUP BY source_name, pred_label
ORDER BY article_count DESC;


Visualization: Clustered bar chart (Source vs Sentiment counts).

3. What percentage of articles are positive vs negative overall?
SELECT 
    pred_label,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_news_final) AS percentage
FROM silver_news_final
GROUP BY pred_label;


Visualization: Donut chart showing sentiment distribution.

4. How does sentiment differ across specific topics (Elections, Economy, Sports, Other)?
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
    FROM silver_news_final
) t
GROUP BY topic, pred_label
ORDER BY topic, article_count DESC;


Visualization: Stacked bar chart by topic and sentiment.

5. Which journalists/authors contribute the most positive or negative articles?
SELECT 
    author,
    pred_label,
    COUNT(*) AS article_count
FROM silver_news_final
WHERE author IS NOT NULL
GROUP BY author, pred_label
ORDER BY article_count DESC
LIMIT 20;


Visualization: Horizontal bar chart (Top 20 authors).

6. How confident is the sentiment model over time?
SELECT 
    date(from_iso8601_timestamp(publishedAt)) AS pub_date,
    AVG(pred_score) AS avg_confidence
FROM silver_news_final
GROUP BY date(from_iso8601_timestamp(publishedAt))
ORDER BY pub_date;


Visualization: Line chart (Date vs Avg Confidence).

7. What is the distribution of sentiment confidence scores?
SELECT 
    pred_label,
    width_bucket(pred_score, 0, 1, 10) AS bucket,
    COUNT(*) AS count
FROM silver_news_final
GROUP BY pred_label, bucket
ORDER BY bucket;


Visualization: Histogram (Confidence buckets).

8. Which sources publish duplicate/repeated content?
SELECT 
    source_name,
    COUNT(DISTINCT title) AS distinct_titles,
    COUNT(*) AS total_articles
FROM silver_news_final
GROUP BY source_name
ORDER BY total_articles DESC;


Visualization: Scatter plot (Distinct vs Total per Source).

📈 Power BI Dashboard

Key visuals created in Power BI:

Sentiment Trend Over Time (line chart)

Sentiment by Source (stacked bar)

Sentiment by Author (top contributors)

Topic-based Sentiment Split (clustered bar)

Confidence Distribution (histogram)

Positive vs Negative Percentage (donut chart)
