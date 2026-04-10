# Hacker News Sentiment Streaming Pipeline

Real-time data pipeline that streams Hacker News stories through Apache Kafka,
performs sentiment analysis with VADER, and visualizes trends in a Streamlit dashboard.

## Architecture

```
Hacker News API        Kafka Producer       Apache Kafka       Kafka Consumer         DuckDB          dbt            Streamlit
(free, no auth)  --->  (Python)       --->  (message queue) --> (Python + VADER) --->  (storage)  -->  (transform) --> (dashboard)
                       Fetches stories       Buffers messages    Sentiment scoring      Raw data       Analytics       4-page UI
                       every 60 seconds      safely              -1.0 to +1.0           tables         models
```

## Tech Stack

| Layer          | Tool                 | Purpose                          |
|----------------|----------------------|----------------------------------|
| Data Source    | Hacker News API      | Free real-time tech news         |
| Message Queue  | Apache Kafka         | Streaming message broker         |
| Processing     | VADER Sentiment      | Rule-based sentiment scoring     |
| Storage        | DuckDB               | Local analytical database        |
| Transformation | dbt                  | SQL models and data quality      |
| Dashboard      | Streamlit + Plotly   | Interactive visualization        |
| Infrastructure | Docker Compose       | Kafka + Zookeeper containers     |

## Dashboard

| Page | What It Shows |
|------|---------------|
| Overview | KPIs, sentiment distribution, category comparison |
| Sentiment Trends | Time-series charts, positive vs negative over time |
| Top Posts | Most positive/negative/popular stories, sentiment vs engagement |
| Category Deep Dive | Detailed category stats, engagement vs sentiment scatter |

## How to Run

### Prerequisites
- Docker Desktop (running)

### One command to start everything
```bash
docker-compose up --build -d

## Project Structure
```
hn-sentiment-pipeline/
├── kafka/
│   ├── producer/hn_producer.py        # HN API → Kafka
│   └── consumer/sentiment_consumer.py # Kafka → VADER → DuckDB
├── dbt_project/
│   ├── models/
│   │   ├── staging/stg_hn_posts.sql
│   │   ├── intermediate/
│   │   │   ├── int_category_hourly.sql
│   │   │   └── int_post_ranked.sql
│   │   └── mart/
│   │       ├── fct_posts.sql
│   │       ├── fct_category_hourly.sql
│   │       └── dim_categories.sql
│   └── tests/
├── dashboard/app.py                   # 4-page Streamlit dashboard
├── docker-compose.yml                 # Kafka infrastructure
└── requirements.txt
```

## Key Concepts Demonstrated

- **Event streaming** with Apache Kafka (producer/consumer pattern)
- **Real-time NLP** with VADER sentiment analysis
- **Data modeling** with dbt (staging → intermediate → mart)
- **Data quality testing** with dbt tests
- **Interactive dashboards** with Streamlit + Plotly
- **Infrastructure as code** with Docker Compose
