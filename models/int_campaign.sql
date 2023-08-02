{{ config(
    materialized='table',
    partition_by={
      "field": "date_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

WITH 

    adwords AS (SELECT * FROM {{ ref('stg_adwords') }} )

    ,facebook AS (SELECT * FROM {{ ref('stg_facebook') }} )

    ,bing AS (SELECT * FROM {{ ref('stg_bing') }} )

    ,criteo AS (SELECT * FROM {{ ref('stg_criteo') }} )

SELECT
    *
    FROM adwords
    UNION ALL
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM bing
    UNION ALL
    SELECT * FROM criteo