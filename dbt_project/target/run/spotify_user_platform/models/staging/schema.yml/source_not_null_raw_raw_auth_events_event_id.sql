
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from "spotify_iam"."main"."raw_auth_events"
where event_id is null



  
  
      
    ) dbt_internal_test