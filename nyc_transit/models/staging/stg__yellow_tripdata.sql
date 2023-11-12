-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE retrieves all data from the 'yellow_tripdata' table in the 'main' source schema.
-- The '{{ source('main', 'yellow_tripdata') }}' is a DBT macro used for referencing a specific source table.
with source as (
    select * from {{ source('main', 'yellow_tripdata') }}
),

-- The 'renamed' CTE
-- This CTE focuses on cleaning, transforming, and filtering data from the 'source' CTE.
-- It involves converting flags to boolean, casting certain columns to appropriate data types, and applying filters.
renamed as (
    select
        -- Selecting various columns without transformation.
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,

        -- Casting 'passenger_count' to an integer type for consistency and accuracy.
        passenger_count::int as passenger_count,

        -- Selecting distance, rate code, and location ID columns as they are.
        trip_distance,
        ratecodeid,
        pulocationid,
        dolocationid,

        -- Converting the 'store_and_fwd_flag' to a boolean value.
        -- This is done using a custom DBT macro 'flag_to_bool' for standardization.
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,

        -- Selecting payment and fare related columns.
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,

        -- Including 'filename' to track the source file of the data.
        filename

    from source
    -- Adding filters to the data selection.
    -- Exclude rows where 'tpep_pickup_datetime' is in the future (post '2022-12-31').
    -- Exclude rows with negative 'trip_distance' as they are not logically valid.
    WHERE tpep_pickup_datetime < TIMESTAMP '2022-12-31'
    AND trip_distance >= 0
    AND passenger_count IS NOT NULL
)

-- Final SELECT statement
-- Retrieves the cleaned, transformed, and filtered data from the 'renamed' CTE.
-- The dataset is now prepared for further analysis, reporting, or integration with other datasets.
select * from renamed
-- End of the SQL script
