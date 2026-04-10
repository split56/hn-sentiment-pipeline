with source as (
    select * from {{ source('raw', 'hn_posts') }}
),

cleaned as (
    select
        post_id,
        lower(category)                             as category,
        title,
        selftext,
        author,
        score,
        num_comments,
        url,
        is_self,

        cast(created_utc as timestamp)              as created_at,
        cast(fetched_at as timestamp)               as fetched_at,
        cast(processed_at as timestamp)             as processed_at,

        cast(cast(created_utc as timestamp) as date)    as created_date,
        extract('hour' from cast(created_utc as timestamp))::int as created_hour,
        strftime(cast(created_utc as timestamp), '%Y-%m-%d %H:00:00') as hour_bucket,

        sentiment_pos                               as sentiment_positive,
        sentiment_neg                               as sentiment_negative,
        sentiment_neu                               as sentiment_neutral,
        sentiment_compound,
        sentiment_label,

        length(title)                               as title_length,
        length(coalesce(selftext, ''))              as body_length

    from source
)

select * from cleaned
