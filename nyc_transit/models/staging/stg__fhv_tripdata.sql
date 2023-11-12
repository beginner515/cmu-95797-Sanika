-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE selects all columns from the 'fhv_tripdata' table located in the 'main' source schema.
-- The '{{ source('main', 'fhv_tripdata') }}' is a DBT macro used for referencing a specific source table.
with source as (
    select * from {{ source('main', 'fhv_tripdata') }}
),

-- The 'renamed' CTE
-- This CTE focuses on cleaning and restructuring the data from the 'source' CTE.
-- It includes transformations such as trimming spaces and converting text to uppercase.
renamed as (
    select
        -- Cleaning up the 'dispatching_base_num' to ensure consistency.
        -- The 'trim' function removes any leading/trailing spaces, and 'upper' converts text to uppercase.
        -- This is important as some IDs might be in lowercase, and we need a standardized format.
        trim(upper(dispatching_base_num)) as dispatching_base_num,

        -- Selecting date-time columns without any transformation.
        pickup_datetime,
        dropoff_datetime,

        -- Selecting location ID columns as they are.
        pulocationid,
        dolocationid,

        -- Skipping the 'sr_flag' column as it is always null and not needed for our analysis.
        -- This is a decision to optimize the dataset for relevant information.

        -- Cleaning up the 'affiliated_base_number' for consistency and standardization.
        trim(upper(affiliated_base_number)) as affiliated_base_number,

        -- Including 'filename' to keep track of the source file of the data.
        filename

    from source
)

-- Final SELECT statement
-- This statement retrieves the cleaned and transformed data from the 'renamed' CTE.
-- The data is now ready for further analysis, reporting, or integration with other datasets.
select * from renamed
-- End of the SQL script