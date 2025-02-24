{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue as (
    select
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as total_revenue
    from {{ ref('fct_taxi_trips') }}
    group by service_type, year, quarter, year_quarter
),
yoy_growth as (
    select
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.year_quarter,
        curr.total_revenue,
        prev.total_revenue as prev_year_revenue,
        case
            when prev.total_revenue is not null
            then (curr.total_revenue - prev.total_revenue) / prev.total_revenue * 100
            else null
        end as yoy_growth_pct
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.year = prev.year + 1
        and curr.quarter = prev.quarter
)
select * from yoy_growth
order by service_type, year, quarter
