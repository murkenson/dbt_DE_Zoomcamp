{{ config(materialized="view", tags=["special"]) }}


select
    {{ dbt_utils.generate_surrogate_key(["vendor_id", "pickup_datetime"]) }} as tripid,
    *

from {{ source("staging_sec", "yellow_tripdata") }}
where vendor_id is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
