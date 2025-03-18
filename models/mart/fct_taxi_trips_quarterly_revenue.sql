with
    quarterly as (
        select
            service_type,
            extract(YEAR from pickup_datetime) as year,
            extract(QUARTER from pickup_datetime) as quarter,
            sum(total_amount) as quarter_revenue
        from {{ ref("fact_trips") }}
        where extract(YEAR from pickup_datetime) in (2019, 2020)
        group by 1, 2, 3
    ),

    base_year as (
        select service_type, year, quarter, quarter_revenue as base_revenue
        from quarterly
        where year = 2019
    )

select
    qr.service_type,
    qr.year,
    qr.quarter,
    qr.quarter_revenue,
    base.base_revenue
from quarterly qr
join
    base_year base
    on base.service_type = qr.service_type
    and base.quarter = qr.quarter


-- calculate quarter_yoy
-- select
--     service_type,
--     year,
--     quarter,
--     (quarter_revenue - base_revenue) / nullif(base_revenue, 0) * 100 as quarter_yoy
-- from quarter_report
-- where year != 2019