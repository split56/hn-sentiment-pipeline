/*
 rank posts by engagement and sentiment within each category
 */

 with posts as (
    select * from {{ ref('stg_hn_posts') }}
),

ranked as (
    select
        *,

        -- Most popular
        row_number() over (
            partition by category order by score desc
        ) as rank_by_score,

        -- Most positive
        row_number() over (
            partition by category order by sentiment_compound desc
        ) as rank_most_positive,

        -- Most negative
        row_number() over (
            partition by category order by sentiment_compound asc
        ) as rank_most_negative

    from posts
)

select * from ranked

