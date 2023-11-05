with source as (
    select * from {{ source('main', 'green_tripdata') }}
),

renamed as (
    select 
        case when VendorID in (1, 2) then VendorID::int else null end as Vendor_ID,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        passenger_count::int as passenger_count,
        trip_distance,
        PULocationID::int as pickup_location_id,
        DOLocationID::int as dropoff_location_id,
        case when RateCodeID in (1, 2, 3, 4, 5, 6) then RateCodeID::int else null end as Rate_Code_ID,
        case 
            when store_and_fwd_flag = 'Y' then true
            when store_and_fwd_flag = 'N' then false
            else null 
        end as store_and_fwd_flag,
        case when payment_type in (1, 2, 3, 4, 5, 6) then payment_type::int else null end as payment_type,
        fare_amount,
        extra,
        mta_tax,
        improvement_surcharge,
        tip_amount,
        tolls_amount,
        total_amount,
        case when trip_type in (1, 2) then trip_type::int else null end as trip_type,
        congestion_surcharge,
        filename
    from source
    where extra >= 0 -- Exclude negative extras
      and mta_tax >= 0 -- Exclude negative MTA tax
      and tip_amount >= 0 -- Exclude negative tips
      and tolls_amount >= 0 -- Exclude negative tolls
      and total_amount >= 0 -- Exclude negative total amounts
      and improvement_surcharge >= 0 -- Exclude negative improvement surcharge
      and (congestion_surcharge >= 0 or congestion_surcharge is null) -- Exclude negative congestion surcharge
    
)

select * from renamed