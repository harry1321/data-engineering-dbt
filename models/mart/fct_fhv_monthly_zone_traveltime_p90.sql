with duration as(
    select
        *,
        timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration
    from {{ref("dim_fhv_trips")}}
)

select
    *,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (
        PARTITION BY year, month, PUlocationID, DOlocationID
    ) AS trip_duration_p90
from duration