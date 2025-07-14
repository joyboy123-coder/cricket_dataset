{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE home_team = 'Bangladesh'
   OR away_team = 'Bangladesh'