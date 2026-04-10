/*
    DIMENSION TABLE: One row per HN category with overall stats.
*/

with posts as (
    select * from {{ ref('stg_hn_posts') }}
),

final as (
    select
        category,

        count(*)                                                        as total_posts,
        sum(score)                                                      as total_score,
        sum(num_comments)                                               as total_comments,

        round(avg(sentiment_compound), 4)                               as avg_sentiment,
        round(avg(sentiment_positive), 4)                               as avg_positive,
        round(avg(sentiment_negative), 4)                               as avg_negative,

        count(case when sentiment_label = 'positive' then 1 end)        as positive_posts,
        count(case when sentiment_label = 'negative' then 1 end)        as negative_posts,
        count(case when sentiment_label = 'neutral' then 1 end)         as neutral_posts,

        round(
            count(case when sentiment_label = 'positive' then 1 end) * 100.0
            / count(*), 1
        )                                                               as positive_pct,

        round(avg(score), 1)                                            as avg_score,
        round(avg(num_comments), 1)                                     as avg_comments,
        min(created_at)                                                 as first_post_at,
        max(created_at)                                                 as last_post_at

    from posts
    group by category
)

select * from final
