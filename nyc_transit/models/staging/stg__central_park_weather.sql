-- Start of the SQL script

-- The 'source' Common Table Expression (CTE)
-- This CTE is used to select all columns from the 'central_park_weather' table in the 'main' source.
-- The '{{ source('main', 'central_park_weather') }}' is a DBT macro to refer to a specific source table.
with source as (
    select * from {{ source('main', 'central_park_weather') }}
),

-- The 'renamed' CTE
-- This CTE is responsible for selecting and renaming specific columns from the 'source' CTE.
-- It also casts certain columns to their appropriate data types for consistency and accuracy.
renamed as (
    select
        -- Selecting and renaming columns. Casting is done for date and numerical columns.
        -- The '::' operator is used for casting in SQL.
        station,
        name,
        date::date as date,  -- Casting 'date' to a date data type.
        awnd::double as awnd, -- Casting 'awnd' to double for precision.
        prcp::double as prcp, -- Casting 'prcp' to double.
        snow::double as snow, -- Casting 'snow' to double.
        snwd::double as snwd, -- Casting 'snwd' to double.
        tmax::int as tmax,    -- Casting 'tmax' to integer.
        tmin::int as tmin,    -- Casting 'tmin' to integer.
        filename              -- Including 'filename' to keep track of data source file.
    from source
)

-- Final SELECT statement
-- This statement selects the transformed and cleaned data from the 'renamed' CTE.
-- It's preparing the final structured data set for further analysis or reporting.
select 
    -- Simply selecting the columns from the 'renamed' CTE.
    -- These columns are now cleaned, transformed, and ready for use.
    date,
    awnd,
    prcp,
    snow,
    snwd,
    tmax,
    tmin,
    filename
from renamed
-- End of the SQL script