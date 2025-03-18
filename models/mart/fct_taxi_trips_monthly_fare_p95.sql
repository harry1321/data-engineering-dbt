with
    valid_trips as (
        select
            service_type,
            fare_amount,
            extract(year from pickup_datetime) as trip_year,
            extract(month from pickup_datetime) as trip_month
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ("Cash", "Credit card")
    )
select distinct
    service_type,
    trip_year,
    trip_month,
    percentile_cont(fare_amount, 0.97) over (
        partition by service_type, trip_year, trip_month
    ) as p97,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type, trip_year, trip_month
    ) as p95,
    percentile_cont(fare_amount, 0.90) over (
        partition by service_type, trip_year, trip_month
    ) as p90
from valid_trips
