{{ config(materialized="incremental", unique_key="tripid") }}

{{
    dbt_utils.log_info(
        "Starting transformation for the green_taxi_2022_incremental table"
    )
}}


select *
from {{ ref("stg_green_tripdata") }}

{% if is_incremental() %}

    where updated_at > (select max(updated_at) from {{ this }})

{% endif %}

    {{
        dbt_utils.log_info(
            "Finished transformation for the green_taxi_2022_incremental table"
        )
    }}
