{{ config(materialized="table") }}

with
    green_tripdata as (
        select
            tripid,
            pickup_datetime,
            dropoff_datetime,
            cast(pickup_location_id as int64) as pickup_location_id,
            cast(dropoff_location_id as int64) as dropoff_location_id,
            'Green' as service_type
        from {{ ref("stg_green_tripdata_sec") }}
    ),
    yellow_tripdata as (
        select
            tripid,
            pickup_datetime,
            dropoff_datetime,
            cast(pickup_location_id as int64) as pickup_location_id,
            cast(dropoff_location_id as int64) as dropoff_location_id,
            'Yellow' as service_type
        from {{ ref("stg_yelllow_tripdata_sec") }}
    ),
    fhv_tripdata_tripdata as (
        select
            tripid,
            pickup_datetime,
            dropoff_datetime,
            cast(pulocationid as int64) as pickup_location_id,
            cast(dolocationid as int64) as dropoff_location_id,
            'FHV' as service_type
        from {{ ref("stg_fhv_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_tripdata
        union all
        select *
        from yellow_tripdata
        union all
        select *
        from fhv_tripdata_tripdata
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    trips_unioned.tripid,
    trips_unioned.service_type,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    trips_unioned.pickup_location_id,
    trips_unioned.dropoff_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from trips_unioned
inner join
    dim_zones as pickup_zone
    on trips_unioned.pickup_location_id = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on trips_unioned.dropoff_location_id = dropoff_zone.locationid
