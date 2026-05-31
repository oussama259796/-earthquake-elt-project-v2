{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT DISTINCT
        earthquake_time
    FROM {{ ref('stg_earthquakes') }}
    WHERE earthquake_time IS NOT NULL
),

enriched AS (
    SELECT
        -- Surrogate Key
        FORMAT_TIMESTAMP('%Y%m%d%H%M%S', earthquake_time) AS time_id,

        earthquake_time,

        -- Date parts
        EXTRACT(YEAR    FROM earthquake_time) AS year,
        EXTRACT(MONTH   FROM earthquake_time) AS month,
        EXTRACT(DAY     FROM earthquake_time) AS day,
        EXTRACT(HOUR    FROM earthquake_time) AS hour,
        EXTRACT(MINUTE  FROM earthquake_time) AS minute,

        -- Derived
        FORMAT_TIMESTAMP('%A', earthquake_time)    AS day_of_week,
        FORMAT_TIMESTAMP('%B', earthquake_time)    AS month_name,
        EXTRACT(DAYOFWEEK FROM earthquake_time) IN (1, 7) AS is_weekend

    FROM source
)

SELECT * FROM enriched