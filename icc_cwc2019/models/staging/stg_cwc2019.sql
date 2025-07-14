{{ config(materialized='view') }}

select *
from {{ source('CRICKET_SCHEMA', 'CRICKET') }}
