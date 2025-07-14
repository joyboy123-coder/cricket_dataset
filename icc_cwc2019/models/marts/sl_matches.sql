{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE home_team = 'Sri Lanka'
   OR away_team = 'Sri Lanka'