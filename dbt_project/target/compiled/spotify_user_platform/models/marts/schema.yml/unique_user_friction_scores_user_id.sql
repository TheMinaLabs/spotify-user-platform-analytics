
    
    

select
    user_id as unique_field,
    count(*) as n_records

from "spotify_iam"."main"."user_friction_scores"
where user_id is not null
group by user_id
having count(*) > 1


