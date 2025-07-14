{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE home_team = 'Afghanistan'
   OR away_team = 'Afghanistan'