{{
    config(
        pre_hook=["{{ create_table_tmp('ny_taxi', 'stg_photo')}}"],
        materialized="table",
        schema="ny_taxi",
    )
}}


{{ dbt_utils.log_info("Starting transformation for the photos table") }}

select *
from ny_rides_marfanyan.photos
limit
    10 {{ dbt_utils.log_info("Starting transformation for the photos table is done") }}
