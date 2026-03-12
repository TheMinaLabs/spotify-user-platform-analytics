
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_ts
from "spotify_iam"."main"."stg_auth_events"
where event_ts is null



  
  
      
    ) dbt_internal_test