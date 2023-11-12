with source as (
    select * from {{ source('main', 'fhv_bases') }}
),

renamed as (
    select 
        base_number,
        trim(base_name) as base_name,
        case 
            when dba is not null then trim(dba)
            else null 
        end as dba,
        trim(dba_category) as dba_category,
        filename
    from source
)

select * from renamed