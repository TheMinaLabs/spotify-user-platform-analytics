
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select friction_score
from "spotify_iam"."main"."user_friction_scores"
where friction_score is null



  
  
      
    ) dbt_internal_test