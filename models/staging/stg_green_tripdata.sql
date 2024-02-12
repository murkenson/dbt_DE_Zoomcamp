{{ config(materialized="view", tags=["special"]) }}


select
    {{ dbt_utils.generate_surrogate_key(["vendorid", "lpep_pickup_datetime"]) }}
    as tripid,
    vendorid as vendorid,
    date(
        timestamp_seconds(cast(lpep_pickup_datetime / 1000000000 as int64))
    ) as pickup_datetime,
    date(
        timestamp_seconds(cast(lpep_dropoff_datetime / 1000000000 as int64))
    ) as dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid as ratecodeid,
    store_and_fwd_flag,
    pulocationid as pulocationid,
    dolocationid as dolocationid,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description,
    fare_amount,
    extra,
    mta_tax,
    ehail_fee,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    cast(trip_type as integer) as trip_type,
    current_timestamp() as updated_at

from {{ source("staging", "green_taxi_2022") }}
where vendorid is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
