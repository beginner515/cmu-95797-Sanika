-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE retrieves all columns from the 'fhvhv_tripdata' table in the 'main' source schema.
-- The '{{ source('main', 'fhvhv_tripdata') }}' is a DBT macro for referencing a specific source table.
with source as (
    select * from {{ source('main', 'fhvhv_tripdata') }}
),

-- The 'renamed' CTE
-- This CTE focuses on cleaning and restructuring data from the 'source' CTE.
-- It includes transformations for text data and integration of custom DBT macros for boolean conversion.
renamed as (
    select
        -- Selecting 'hvfhs_license_num' without transformation.
        hvfhs_license_num,

        -- Cleaning up 'dispatching_base_num' and 'originating_base_num' columns.
        -- The 'trim' function removes leading/trailing spaces, and 'upper' converts text to uppercase.
        trim(upper(dispatching_base_num)) as dispatching_base_num,
        trim(upper(originating_base_num)) as originating_base_num,

        -- Selecting various datetime columns without transformation.
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,

        -- Selecting location ID and trip detail columns as they are.
        pulocationid,
        dolocationid,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,

        -- Converting flag columns to boolean values using custom DBT macros 'flag_to_bool'.
        -- This standardizes the representation of these flags across the dataset.
        {{flag_to_bool("shared_request_flag")}} as shared_request_flag,
        {{flag_to_bool("shared_match_flag")}} as shared_match_flag,
        {{flag_to_bool("access_a_ride_flag")}} as access_a_ride_flag,
        {{flag_to_bool("wav_request_flag")}} as wav_request_flag,
        {{flag_to_bool("wav_match_flag",)}} as wav_match_flag,

        -- Including 'filename' to track the data source file.
        filename

    from source
)

-- Final SELECT statement
-- Retrieves the cleaned and transformed data from the 'renamed' CTE.
-- The dataset is now prepared for further analysis or integration with other datasets.
select * from renamed
-- End of the SQL script
