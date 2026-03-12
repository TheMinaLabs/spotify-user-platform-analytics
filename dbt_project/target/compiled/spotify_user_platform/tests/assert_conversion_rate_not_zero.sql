-- tests/assert_conversion_rate_not_zero.sql
-- Singular test: fail if overall login conversion rate drops below 10%.
-- A value this low would indicate a pipeline or auth system outage.

select
    metric_date,
    round(avg(login_conversion_rate), 4) as avg_daily_conversion
from "spotify_iam"."main"."fct_daily_auth_metrics"
where metric_date >= current_date - interval '7 days'
  and login_conversion_rate is not null
group by 1
having avg(login_conversion_rate) < 0.10