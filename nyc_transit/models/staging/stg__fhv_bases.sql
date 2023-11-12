-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE selects all columns from the 'fhv_bases' table located in the 'main' source schema.
-- The '{{ source('main', 'fhv_bases') }}' is a DBT macro used for referencing a specific source table.
with source as (
    select * from {{ source('main', 'fhv_bases') }}
),

-- The 'renamed' CTE
-- This CTE is focused on cleaning and restructuring the data from the 'source' CTE.
-- It includes data transformations such as trimming and changing the case of text.
renamed as (
    select
        -- Clean up the 'base_number' column to ensure consistency, especially for use as foreign keys.
        -- 'trim' function removes any leading/trailing spaces, and 'upper' function converts text to uppercase.
        trim(upper(base_number)) as base_number,

        -- Selecting additional columns as they are without transformation.
        base_name,
        dba,
        dba_category,

        -- Including 'filename' to track the source file of the data.
        filename

    from source
)

-- Final SELECT statement
-- This statement retrieves the cleaned and transformed data from the 'renamed' CTE.
-- It's now prepared for further analysis, reporting, or integration with other datasets.
select * from renamed
-- End of the SQL script