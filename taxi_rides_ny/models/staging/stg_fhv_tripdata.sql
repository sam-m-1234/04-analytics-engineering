{{
    config(
        materialized='view'
    )
}}

with tripdata as
(
  select * from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null
)
select
    -- identifiers
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
