with source as (
    select * from {{ source('main', 'bike_data') }}
),

renamed as (
    select 
        tripduration::int as trip_duration,
        COALESCE(starttime::timestamp, started_at::timestamp) as start_time,
        COALESCE(stoptime::timestamp, ended_at::timestamp)  as stop_time,
        COALESCE("start station id"::int, start_station_id::int) as start_station_id,
        COALESCE("end station id"::int, end_station_id::int) as end_station_id,
        COALESCE("start station name", start_station_name) as start_station_name,
        COALESCE("end station name", end_station_name) as end_station_name,
        COALESCE("start station latitude"::double, start_lat::double) as start_station_latitude,
        COALESCE("start station longitude"::double, start_lng::double) as start_station_longitude,
        COALESCE("end station latitude"::double, end_lat::double) as end_station_latitude,
        COALESCE("end station longitude"::double, end_lng::double) as end_station_longitude,
        bikeid::int as bike_id,
        usertype as user_type,
        "birth year"::int as birth_year,
        case 
            when gender = '0' then 'unknown'
            when gender = '1' then 'male'
            when gender = '2' then 'female'
            else 'unknown'
        end as gender,
        ride_id,
        rideable_type,
        member_casual,
        filename
    from source
)

select * from renamed