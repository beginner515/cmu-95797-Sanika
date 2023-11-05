with source as (
    select * from {{ source('main', 'yellow_tripdata') }}
),

renamed as (
    select 
        case when VendorID in (1, 2) then VendorID::int else null end as Vendor_ID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count::int as passenger_count,
        trip_distance,
        PULocationID::int as pickup_location_id,
        DOLocationID::int as dropoff_location_id,
        case when RatecodeID in (1, 2, 3, 4, 5, 6) then RatecodeID::int else null end as Rate_Code_ID,
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
        congestion_surcharge,
        airport_fee,
        filename
    from source
    where extra >= 0 -- Excluding negative extras
      and (airport_fee >= 0 or airport_fee is null) -- airport fee cannot be negative
      and mta_tax >= 0 -- Excluding negative MTA tax
      and tip_amount >= 0 -- Excluding negative tips
      and tolls_amount >= 0 -- Excluding negative tolls
      and total_amount >= 0 -- Excluding negative total amounts
      and improvement_surcharge >= 0 -- Excluding negative improvement surcharge
      and (congestion_surcharge >= 0 or congestion_surcharge is null) -- Excluding negative congestion surcharge
)

select * from renamed