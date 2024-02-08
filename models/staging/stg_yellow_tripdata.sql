{{ config(materialized="view") }}


select
    {{ dbt_utils.generate_surrogate_key(["vendorid", "tpep_pickup_datetime"]) }}
    as tripid,
    vendorid as vendorid,
   tpep_pickup_datetime as pickup_datetime,
   tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid as ratecodeid,
    store_and_fwd_flag,
    pulocationid as pulocationid,
    dolocationid as dolocationid,
    payment_type as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description,
    fare_amount,
    extra,
    mta_tax,
    0 as ehail_fee,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    0 as trip_type

from {{ source("staging", "yellow_cab_data") }}
where vendorid is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %} 
        limit 100
{% endif %}
