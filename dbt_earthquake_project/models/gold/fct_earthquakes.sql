{{
    config(
        materialized='table',
        unique_key='quake_id'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_earthquakes') }}
),

final AS (
    SELECT
        -- Keys
        s.quake_id,
        {{ dbt_utils.generate_surrogate_key(['s.longitude', 's.latitude']) }} AS location_id,
        FORMAT_TIMESTAMP('%Y%m%d%H%M%S', s.earthquake_time) AS time_id,

        -- Measures
        s.magnitude,
        s.depth,

        -- Attributes
        s.status,
        s.tsunami,
        s.earthquake_type,

        -- Severity
        CASE
            WHEN s.magnitude < 2.0 THEN 'micro'
            WHEN s.magnitude < 4.0 THEN 'minor'
            WHEN s.magnitude < 6.0 THEN 'moderate'
            WHEN s.magnitude < 7.0 THEN 'strong'
            WHEN s.magnitude < 8.0 THEN 'major'
            ELSE 'great'
        END AS magnitude_category,

        -- Metadata
        s.source_url,
        s.ingested_at

    FROM source s
)

SELECT * FROM final