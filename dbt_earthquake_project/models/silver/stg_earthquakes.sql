    {{
      config(
        materialized = 'table',
        unique_key= 'quake_id',
        on_schema_change= 'sync_all_columns'
        )
    }}

WITH source AS(
    SELECT
        raw_payload,
        source_url,
        ingested_at
    FROM {{ source('bronze_layer', 'raw_earthquakes') }}

),
unnested AS(
    SELECT
        source_url,
        ingested_at,
        feature
    FROM source,
    UNNEST(JSON_QUERY_ARRAY(SAFE.PARSE_JSON(TO_JSON_STRING(raw_payload)), '$.features')) AS feature),
parsed AS(
    SELECT
        --id
        JSON_VALUE(feature , '$.id') AS quake_id,

        -- Measurements
        SAFE_CAST(JSON_VALUE(feature, '$.properties.mag')AS numeric) AS magnitude,
        SAFE_CAST(JSON_VALUE(feature, '$.geometry.coordinates[0]')AS numeric)AS longitude ,
        SAFE_CAST(JSON_VALUE(feature, '$.geometry.coordinates[1]')AS numeric)AS latitude ,
        SAFE_CAST(JSON_VALUE(feature, '$.geometry.coordinates[2]')AS numeric)AS depth ,

        -- Time
        TIMESTAMP_MILLIS(
            SAFE_CAST(JSON_VALUE(feature, '$.properties.time')AS INT64)) AS  earthquake_time,
        TIMESTAMP_MILLIS(
            SAFE_CAST(JSON_VALUE(feature, '$.properties.updated')AS INT64)) AS updated_at,

        -- Attributes

        JSON_VALUE(feature, '$.properties.status') AS status,
        JSON_VALUE(feature, '$.properties.place') AS place,
        SAFE_CAST(JSON_VALUE(feature, '$.properties.tsunami')AS INT64) AS tsunami,
        JSON_VALUE(feature, '$.properties.type') AS earthquake_type,

        -- Metadata

        source_url,
        ingested_at

    FROM unnested
),
quality_checked AS(
    SELECT
        quake_id,
        CASE
            WHEN magnitude<= 0 THEN NULL
            ELSE magnitude
        END AS magnitude,

        longitude,
        latitude,

        CASE
            WHEN depth < 0 THEN NULL
            ELSE depth
        END AS depth,

        CASE
            WHEN earthquake_time IS NULL THEN ingested_at
            WHEN earthquake_time > CURRENT_TIMESTAMP THEN ingested_at
            ELSE earthquake_time
        END AS earthquake_time,

        updated_at,
        status,
        place,
        tsunami,
        earthquake_type,
        source_url,
        ingested_at
        FROM parsed
        WHERE quake_id IS NOT NULL 
        AND magnitude IS NOT NULL
),
deduplicated AS (
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY quake_id ORDER BY ingested_at DESC) as row_num
        FROM quality_checked
    )
    WHERE row_num = 1
)
SELECT * FROM deduplicated


