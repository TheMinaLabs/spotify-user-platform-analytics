
    
    

with all_values as (

    select
        friction_segment as value_field,
        count(*) as n_records

    from "spotify_iam"."main"."user_friction_scores"
    group by friction_segment

)

select *
from all_values
where value_field not in (
    'high_friction','medium_friction','low_friction'
)


