-- compound sentiment must be between -1 and 1
select post_id, sentiment_compound
from {{ ref('fct_posts') }}
where sentiment_compound < -1 or sentiment_compound > 1