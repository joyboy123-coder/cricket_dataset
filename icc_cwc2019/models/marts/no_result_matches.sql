{{ config(materialized='view') }} 

SELECT *
FROM {{ ref('stg_cwc2019') }}
WHERE winner = 'No Result'