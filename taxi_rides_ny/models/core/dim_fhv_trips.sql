{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_tripdata.pickup_location_id,
    fhv_tripdata.dropoff_location_id,

    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,

    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,

    -- **New Time Dimensions**
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(QUARTER FROM fhv_tripdata.pickup_datetime) AS quarter,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS month,
    CONCAT(EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime), '/Q', EXTRACT(QUARTER FROM fhv_tripdata.pickup_datetime)) AS year_quarter,
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_location_id = dropoff_zone.locationid
