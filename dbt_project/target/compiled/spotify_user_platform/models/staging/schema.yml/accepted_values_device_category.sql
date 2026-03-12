
    
    

with all_values as (

    select
        device_category as value_field,
        count(*) as n_records

    from "spotify_iam"."main"."stg_auth_events"
    group by device_category

)

select *
from all_values
where value_field not in (
    'mobile','desktop','web','smart_tv','unknown'
)


