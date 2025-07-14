{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE home_team = 'West Indies'
   OR away_team = 'West Indies'