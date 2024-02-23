{{
    config(
        pre_hook=["{{ create_table_tmp('dbt_marfanian', 'stg_photo')}}"],
        materialized="table",
    )
}}


{{ dbt_utils.log_info("Starting transformation for the photos table") }}

select *
from ny_rides_marfanyan.photos
limit
    10 {{ dbt_utils.log_info("Starting transformation for the photos table is done") }}
