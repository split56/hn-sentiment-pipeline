/*
    FACT TABLE: Hourly sentiment and volume by category.
    Powers time-series charts.
*/

select * from {{ ref('int_category_hourly') }}
