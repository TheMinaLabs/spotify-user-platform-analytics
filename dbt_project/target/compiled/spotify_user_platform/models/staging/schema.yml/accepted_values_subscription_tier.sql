
    
    

with all_values as (

    select
        subscription_tier as value_field,
        count(*) as n_records

    from "spotify_iam"."main"."stg_users"
    group by subscription_tier

)

select *
from all_values
where value_field not in (
    'free','premium','unknown'
)


