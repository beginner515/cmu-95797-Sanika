with source as (
    select * from {{ source('main', 'fhv_tripdata') }}
),

renamed as (
    select 
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID::int as pickup_location_id,
        DOlocationID::int as dropoff_location_id,
        Affiliated_base_number,
        filename
    from source
)

select * from renamed