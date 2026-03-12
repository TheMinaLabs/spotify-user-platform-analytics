-- tests/assert_no_future_events.sql
-- Singular test: fail if any auth event has a timestamp in the future.
-- Catches upstream pipeline delays or data quality issues.

select
    event_id,
    event_ts,
    current_timestamp as check_ts
from {{ ref('stg_auth_events') }}
where event_ts > current_timestamp
