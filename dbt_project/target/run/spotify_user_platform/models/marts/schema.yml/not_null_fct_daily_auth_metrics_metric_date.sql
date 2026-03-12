
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric_date
from "spotify_iam"."main"."fct_daily_auth_metrics"
where metric_date is null



  
  
      
    ) dbt_internal_test