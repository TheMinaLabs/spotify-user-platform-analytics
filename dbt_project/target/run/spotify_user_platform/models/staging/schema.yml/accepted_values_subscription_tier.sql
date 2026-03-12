
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test