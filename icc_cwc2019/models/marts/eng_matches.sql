{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE home_team = 'England'
   OR away_team = 'England'