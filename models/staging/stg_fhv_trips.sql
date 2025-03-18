select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null 