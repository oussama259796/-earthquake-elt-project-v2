{{
    config(
        materialized='view'
    )
}}

SELECT 
    f.quake_id,
    f.magnitude,
    f.depth,
    l.place,
    l.region,
    l.latitude,
    l.longitude,
    t.earthquake_time,
    t.year,
    t.month_name
FROM {{ ref('fct_earthquakes') }} f
LEFT JOIN {{ ref('dim_locations') }} l ON f.location_id = l.location_id
LEFT JOIN {{ ref('dim_time') }} t ON f.time_id = t.time_id