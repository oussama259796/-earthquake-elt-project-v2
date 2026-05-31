{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT DISTINCT
        quake_id,
        place,
        longitude,
        latitude,
    FROM {{ ref('stg_earthquakes') }}
    WHERE place IS NOT NULL
),

enriched AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['longitude', 'latitude']) }} AS location_id,

        -- Attributes
        place,
        ROUND(longitude, 6)  AS longitude,
        ROUND(latitude, 6)   AS latitude,
        

        

        CASE
            WHEN longitude BETWEEN -180 AND -30 THEN 'Americas'
            WHEN longitude BETWEEN -30  AND  60 THEN 'Europe/Africa'
            WHEN longitude BETWEEN  60  AND 180 THEN 'Asia/Pacific'
        END                  AS region

    FROM source
)

SELECT * FROM enriched