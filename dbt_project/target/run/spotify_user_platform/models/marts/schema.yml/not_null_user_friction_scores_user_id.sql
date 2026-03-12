
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from "spotify_iam"."main"."user_friction_scores"
where user_id is null



  
  
      
    ) dbt_internal_test