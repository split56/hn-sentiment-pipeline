-- HN scores should never be negative
select post_id, score
from {{ ref('fct_posts') }}
where score < 0
