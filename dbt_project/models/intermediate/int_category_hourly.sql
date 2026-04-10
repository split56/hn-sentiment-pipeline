/*
 aggregate sentiment and activity per category per hour
 */

 with posts as (
     select * from {{ ref('stg_hn_posts') }}
 ),

hourly as (
    select
        category,
        hour_bucket,
        created_date,
        created_hour,

        -- Volume
        count(*)                                                    as post_count,
        sum(score)                                                  as total_score,
        sum(num_comments)                                           as total_comments,

        -- Sentiment
        round(avg(sentiment_compound), 4)                           as avg_sentiment,
        round(avg(sentiment_positive), 4)                           as avg_positive,
        round(avg(sentiment_negative), 4)                           as avg_negative,

        -- Distribution
        count(case when sentiment_label = 'positive' then 1 end)   as positive_count,
        count(case when sentiment_label = 'negative' then 1 end)   as negative_count,
        count(case when sentiment_label = 'neutral' then 1 end)    as neutral_count,

        -- Engagement
        count(case when score > 100 then 1 end)                    as hot_posts,
        max(score)                                                  as max_score

    from posts
    group by category, hour_bucket, created_date, created_hour
)

select * from hourly