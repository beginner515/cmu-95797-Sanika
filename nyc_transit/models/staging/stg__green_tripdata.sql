-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE fetches all data from the 'green_tripdata' table in the 'main' source schema.
-- The '{{ source('main', 'green_tripdata') }}' is a DBT macro used to reference a specific source table.
with source as (
    select * from {{ source('main', 'green_tripdata') }}
),

-- The 'renamed' CTE
-- This CTE is focused on cleaning, transforming, and filtering the data from the 'source' CTE.
-- It includes converting flags to boolean and casting certain columns to appropriate data types.
renamed as (
    select
        -- Selecting various columns without transformation.
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,

        -- Converting the 'store_and_fwd_flag' to a boolean value for consistency.
        -- This is done using a custom DBT macro 'flag_to_bool'.
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,        

        -- Selecting additional columns as they are.
        ratecodeid,
        pulocationid,
        dolocationid,

        -- Casting 'passenger_count' to an integer type.
        passenger_count::int as passenger_count,

        -- Selecting fare and trip related columns without transformation.
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,

        -- The 'ehail_fee' column is removed because it contains 100% null values.
        -- This decision helps optimize the dataset.
        --ehail_fee,

        -- Selecting additional columns related to charges and payment type.
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,

        -- Including 'filename' to track the source file of the data.
        filename

    from source
    -- Adding filters to the data selection.
    -- Exclude rows where 'lpep_pickup_datetime' is in the future (post '2022-12-31').
    -- Exclude rows with negative 'trip_distance' as it's not logically valid.
    WHERE lpep_pickup_datetime < TIMESTAMP '2022-12-31' 
    AND trip_distance >= 0
    AND passenger_count IS NOT NULL
)

-- Final SELECT statement
-- Retrieves the cleaned, transformed, and filtered data from the 'renamed' CTE.
-- The dataset is now ready for further analysis or reporting.
select * from renamed
-- End of the SQL script
