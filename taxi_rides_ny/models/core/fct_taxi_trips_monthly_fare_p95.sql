{{ config(materialized='table') }}

WITH filtered_trips AS (
    SELECT
        service_type,
        year,
        month,
        fare_amount
    FROM {{ ref('fct_taxi_trips') }}
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND LOWER(payment_type_description) IN ('cash', 'credit card')
),
percentiles AS (
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS fare_p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS fare_p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS fare_p90
    FROM filtered_trips
)
SELECT DISTINCT
    service_type,
    year,
    month,
    fare_p97,
    fare_p95,
    fare_p90
FROM percentiles
WHERE year = 2020 AND month = 4
ORDER BY service_type, year, month
