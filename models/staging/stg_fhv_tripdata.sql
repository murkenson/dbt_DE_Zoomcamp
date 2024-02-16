{{ config(materialized="view", tags=["special"]) }}

{{
    dbt_utils.log_info(
        "Initiating a model with filtering by the 2019 year in the stg_fhv_trip datatable"
    )
}}

select
    dispatching_base_num as dispatching_base_num,
    pickup_datetime as pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    pulocationid as pulocationid,
    dolocationid as dolocationid,
    sr_flag as sr_flag,
    affiliated_base_number as affiliated_base_number

from {{ source("staging_sec", "fhv_tripdata") }}
where EXTRACT(YEAR FROM pickup_datetime) = 2019

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}


{{
    dbt_utils.log_info(
        "Initiation of the 2019 filtered model in the stg_fhv_trip datatable is completed"
    )
}}