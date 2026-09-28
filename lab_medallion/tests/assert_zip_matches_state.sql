{{ config(severity='warn') }}

-- The first 3 digits of the ZIP code must belong to a range of the state
select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('state_zip_prefixes') }} r
    where r.state = u.state
      and try_cast(left(u.zip_code, 3) as integer) between r.prefix_min and r.prefix_max
)
