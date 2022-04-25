{{ config(materialized='view')}}

SELECT * FROM {{ source('staging','pbp_data_partitioned') }}
LIMIT 100