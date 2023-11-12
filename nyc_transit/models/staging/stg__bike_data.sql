-- Starting the SQL script with a Common Table Expression (CTE) named 'source'.
-- This CTE extracts all data from the 'bike_data' table located in the 'main' source schema.
-- The '{{ source('main', 'bike_data') }}' is a DBT macro to reference a specific source table.
with source as (
    select * from {{ source('main', 'bike_data') }}
),

-- Defining another CTE named 'renamed'.
-- This CTE renames and restructures the data from the 'source' CTE.
-- It selects specific columns for further processing and analysis.
renamed as (
    select
        -- Selecting and listing out the columns from the 'source' CTE.
        -- These columns are directly used as they are, without any transformation.
        tripduration,
        starttime,
        stoptime,
        "start station id",
        "start station name",
        "start station latitude",
        "start station longitude",
        "end station id",
        "end station name",
        "end station latitude",
        "end station longitude",
        bikeid,
        usertype,
        "birth year",
        gender,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        filename
    from source
)

-- Final SELECT statement that performs data transformation.
-- This statement combines and standardizes data from various columns.
-- It uses the COALESCE function to handle possible NULL values in the dataset.
select
    -- Creating a unified timestamp column for the start time of the trip.
    -- If 'starttime' is NULL, 'started_at' is used as a fallback.
    coalesce(starttime, started_at)::timestamp as started_at_ts,

    -- Creating a unified timestamp column for the end time of the trip.
    -- If 'stoptime' is NULL, 'ended_at' is used as a fallback.
    coalesce(stoptime, ended_at)::timestamp as ended_at_ts,

    -- Calculating 'tripduration' if it's not already provided.
    -- This is done by finding the difference in seconds between 'started_at_ts' and 'ended_at_ts'.
    coalesce(tripduration::int,datediff('second', started_at_ts, ended_at_ts)) tripduration,

    -- Standardizing station IDs, names, latitudes, and longitudes.
    -- Combines similar columns from the original dataset into a single column each.
    coalesce("start station id", start_station_id) as start_station_id,  
    coalesce("start station name", start_station_name) as start_station_name,
    coalesce("start station latitude", start_lat)::double as start_lat,
    coalesce("start station longitude", start_lng)::double as start_lng, 
    coalesce("end station id", end_station_id) as end_station_id,  
    coalesce("end station name", end_station_name) as end_station_name,
    coalesce("end station latitude", end_lat)::double as end_lat,
    coalesce("end station longitude", end_lng)::double as end_lng,

    -- Including the 'filename' column from the 'renamed' CTE.
    -- This could be useful for tracking the source file of the data.
    filename
from renamed
-- End of the SQL script