/*
    FACT TABLE: One row per HN story with all metrics and rankings.
    Primary table for the dashboard.
*/

select
    post_id,
    category,
    title,
    selftext,
    author,
    score,
    num_comments,
    url,
    is_self,
    created_at,
    created_date,
    created_hour,
    hour_bucket,
    sentiment_positive,
    sentiment_negative,
    sentiment_neutral,
    sentiment_compound,
    sentiment_label,
    title_length,
    body_length,
    rank_by_score,
    rank_most_positive,
    rank_most_negative

from {{ ref('int_post_ranked') }}
