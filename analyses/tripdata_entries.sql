with tripdata_entries as (

  select *
  from {{ ref('stg_green_tripdata') }}

)

select
  sum(passenger_count) over (partition by tripid order by vendorid rows unbounded preceding)
from tripdata_entries
order by tripid, vendorid