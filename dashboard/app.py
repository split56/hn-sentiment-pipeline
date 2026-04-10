"""
hacker news sentiment dashboard
run : streamlit run dashboard/app.py
"""

import os
import sys
from pathlib import Path
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "hn_sentiment.db"))

st.set_page_config(page_title="HN Sentiment", layout="wide")
st_autorefresh(interval=30 * 1000, key="data_refresh")

# query helper
@st.cache_data(ttl=60)
def query(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df

# detect schema name
# Use raw table for real-time data, fall back to dbt mart tables
USE_REALTIME = True

if USE_REALTIME:
    # Create a view that mirrors what dbt would produce, but always fresh
    def setup_realtime_views():
        con = duckdb.connect(DB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS realtime")
        con.execute("""
            CREATE OR REPLACE VIEW realtime.fct_posts AS
            SELECT
                post_id,
                lower(category) as category,
                title,
                selftext,
                author,
                score,
                num_comments,
                url,
                is_self,
                cast(created_utc as timestamp) as created_at,
                cast(cast(created_utc as timestamp) as date) as created_date,
                extract('hour' from cast(created_utc as timestamp))::int as created_hour,
                strftime(cast(created_utc as timestamp), '%Y-%m-%d %H:00:00') as hour_bucket,
                sentiment_pos as sentiment_positive,
                sentiment_neg as sentiment_negative,
                sentiment_neu as sentiment_neutral,
                sentiment_compound,
                sentiment_label,
                length(title) as title_length,
                length(coalesce(selftext, '')) as body_length,
                row_number() over (partition by category order by score desc) as rank_by_score,
                row_number() over (partition by category order by sentiment_compound desc) as rank_most_positive,
                row_number() over (partition by category order by sentiment_compound asc) as rank_most_negative
            FROM raw.hn_posts
        """)
        con.execute("""
            CREATE OR REPLACE VIEW realtime.dim_categories AS
            SELECT
                lower(category) as category,
                count(*) as total_posts,
                sum(score) as total_score,
                sum(num_comments) as total_comments,
                round(avg(sentiment_compound), 4) as avg_sentiment,
                round(avg(sentiment_pos), 4) as avg_positive,
                round(avg(sentiment_neg), 4) as avg_negative,
                count(case when sentiment_label = 'positive' then 1 end) as positive_posts,
                count(case when sentiment_label = 'negative' then 1 end) as negative_posts,
                count(case when sentiment_label = 'neutral' then 1 end) as neutral_posts,
                round(count(case when sentiment_label = 'positive' then 1 end) * 100.0 / count(*), 1) as positive_pct,
                round(avg(score), 1) as avg_score,
                round(avg(num_comments), 1) as avg_comments,
                min(cast(created_utc as timestamp)) as first_post_at,
                max(cast(created_utc as timestamp)) as last_post_at
            FROM raw.hn_posts
            GROUP BY lower(category)
        """)
        con.close()

    setup_realtime_views()
    SCHEMA = "realtime"
else:
    SCHEMA = None
    for candidate in ["main_analytics", "analytics"]:
        try:
            query(f"SELECT 1 FROM {candidate}.fct_posts LIMIT 1")
            SCHEMA = candidate
            break
        except:
            continue

if SCHEMA is None:
    st.error("No data found. Make sure the consumer is running.")
    st.stop()


# sidebar
st.sidebar.title("HN Sentiment")
st.sidebar.markdown("Hacker News + Kafka + VADER")
st.sidebar.markdown("---")

cats = query(f"SELECT DISTINCT category FROM {SCHEMA}.fct_posts ORDER BY 1")
all_cats = cats["category"].tolist()
selected_cats = st.sidebar.multiselect("Categories", all_cats, default=all_cats)

cat_filter = (
    f"category IN ({','.join([repr(c) for c in selected_cats])})"
    if selected_cats else "1=1"
)

st.sidebar.markdown("---")
st.sidebar.markdown("Built with Kafka + DuckDB + dbt + Streamlit")

page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Sentiment Trends", "Top Posts", "Category Deep Dive"]
)

# Overview
if page == "Overview":
    st.title("Hacker News Sentiment Overview")

    kpis = query(f"""
        SELECT
            COUNT(*)                          AS total_posts,
            ROUND(AVG(sentiment_compound), 3) AS avg_sentiment,
            SUM(score)                        AS total_upvotes,
            SUM(num_comments)                 AS total_comments,
            COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) AS positive,
            COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) AS negative,
            COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END)  AS neutral_count
        FROM {SCHEMA}.fct_posts
        WHERE {cat_filter}
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Stories", f"{int(kpis['total_posts'][0]):,}")
    c2.metric("Avg Sentiment", f"{kpis['avg_sentiment'][0]:+.3f}")
    c3.metric("Total Upvotes", f"{int(kpis['total_upvotes'][0]):,}")
    c4.metric("Total Comments", f"{int(kpis['total_comments'][0]):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Sentiment Distribution")
        dist = query(f"""
                SELECT sentiment_label, COUNT(*) AS count
                FROM {SCHEMA}.fct_posts WHERE {cat_filter}
                GROUP BY sentiment_label
            """)
        fig = px.pie(dist, names="sentiment_label", values="count",
                     color="sentiment_label",
                     color_discrete_map={
                         "positive": "#2ecc71",
                         "negative": "#e74c3c",
                         "neutral": "#95a5a6"
                     })
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Sentiment by Category")
        cat_sent = query(f"""
                SELECT category,
                       ROUND(AVG(sentiment_compound), 3) AS avg_sentiment,
                       COUNT(*) AS posts
                FROM {SCHEMA}.fct_posts WHERE {cat_filter}
                GROUP BY category ORDER BY avg_sentiment DESC
            """)
        fig = px.bar(cat_sent, x="category", y="avg_sentiment",
                     color="avg_sentiment",
                     color_continuous_scale=["#e74c3c", "#f1c40f", "#2ecc71"],
                     color_continuous_midpoint=0)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sentiment Score Distribution")
    scores = query(f"""
            SELECT sentiment_compound FROM {SCHEMA}.fct_posts WHERE {cat_filter}
        """)
    fig = px.histogram(scores, x="sentiment_compound", nbins=50,
                       color_discrete_sequence=["#3498db"])
    fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Neutral")
    st.plotly_chart(fig, use_container_width=True)

# sentiment trends
elif page == "Sentiment Trends":
    st.title("Sentiment Over Time")

    trend = query(f"""
        SELECT hour_bucket, category,
               ROUND(AVG(sentiment_compound), 3) AS avg_sentiment,
               COUNT(*) AS posts
        FROM {SCHEMA}.fct_posts WHERE {cat_filter}
        GROUP BY hour_bucket, category
        ORDER BY hour_bucket
    """)

    if not trend.empty:
        fig = px.line(trend, x="hour_bucket", y="avg_sentiment", color="category",
                      labels={"hour_bucket": "Time", "avg_sentiment": "Avg Sentiment"},
                      markers=True)
        fig.add_hline(y = 0, line_dash = "dash", line_color = "gray",)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trend data available")

    st.subheader("Post Volumn Over Time")
    vol = query(
        f"""
            SELECT hour_bucket, COUNT(*) AS posts
            FROM {SCHEMA}.fct_posts WHERE {cat_filter}
            GROUP BY hour_bucket ORDER BY hour_bucket
        """
    )
    if not vol.empty:
        fig = px.bar(vol, x="hour_bucket", y="posts",
                     labels={"hour_bucket": "Time", "posts": "Posts"},
                     color_discrete_sequence=["#3498db"])
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Positive vs Negative Over Time")
    pn = query(f"""
            SELECT hour_bucket,
                   COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) AS positive,
                   COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) AS negative
            FROM {SCHEMA}.fct_posts WHERE {cat_filter}
            GROUP BY hour_bucket ORDER BY hour_bucket
        """)
    if not pn.empty:
        fig = px.bar(pn, x="hour_bucket", y=["positive", "negative"],
                     barmode="group",
                     color_discrete_map={"positive": "#2ecc71", "negative": "#e74c3c"})
        st.plotly_chart(fig, use_container_width=True)

# top posts
elif page == "Top Posts":
    st.title("Top Stories")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Most Positive Stories")
        pos = query(f"""
            SELECT category, title, sentiment_compound, score, num_comments
            FROM {SCHEMA}.fct_posts WHERE {cat_filter}
            ORDER BY sentiment_compound DESC LIMIT 15
        """)
        st.dataframe(pos, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Most Negative Stories")
        neg = query(f"""
            SELECT category, title, sentiment_compound, score, num_comments
            FROM {SCHEMA}.fct_posts WHERE {cat_filter}
            ORDER BY sentiment_compound ASC LIMIT 15
        """)
        st.dataframe(neg, use_container_width=True, hide_index=True)

    st.subheader("Most Popular (Highest Score)")
    top = query(f"""
        SELECT category, title, score, num_comments,
               sentiment_label, sentiment_compound
        FROM {SCHEMA}.fct_posts WHERE {cat_filter}
        ORDER BY score DESC LIMIT 20
    """)
    st.dataframe(top, use_container_width=True, hide_index=True)

    st.subheader("Sentiment vs Engagement")
    scatter = query(f"""
        SELECT category, score, num_comments, sentiment_compound, title
        FROM {SCHEMA}.fct_posts
        WHERE {cat_filter} AND score > 0
    """)
    if not scatter.empty:
        fig = px.scatter(scatter, x="sentiment_compound", y="score",
                         color="category", size="num_comments",
                         hover_data=["title"],
                         labels={"sentiment_compound": "Sentiment", "score": "Upvotes"})
        st.plotly_chart(fig, use_container_width=True)

# category deep dive
elif page == "Category Deep Dive":
    st.title("Category Comparison")

    stats = query(f"""
            SELECT * FROM {SCHEMA}.dim_categories WHERE {cat_filter}
            ORDER BY total_posts DESC
        """)
    st.dataframe(stats, use_container_width=True, hide_index=True)

    st.subheader("Sentiment Breakdown by Category")
    if not stats.empty:
        fig = px.bar(stats, x="category",
                     y=["positive_posts", "negative_posts", "neutral_posts"],
                     barmode="stack",
                     color_discrete_map={
                         "positive_posts": "#2ecc71",
                         "negative_posts": "#e74c3c",
                         "neutral_posts": "#95a5a6"
                     })
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Engagement vs Sentiment")
    if not stats.empty:
        fig = px.scatter(stats, x="avg_sentiment", y="avg_score",
                         size="total_posts", text="category",
                         labels={"avg_sentiment": "Avg Sentiment", "avg_score": "Avg Score"})
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data Explorer")
    sample = query(f"""
            SELECT post_id, category, title, score, num_comments,
                   sentiment_label, sentiment_compound, created_at
            FROM {SCHEMA}.fct_posts WHERE {cat_filter}
            ORDER BY created_at DESC LIMIT 100
        """)
    st.dataframe(sample, use_container_width=True, hide_index=True)

