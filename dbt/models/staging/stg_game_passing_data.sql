{{ config(materialized='view')}}

SELECT posteam, game_id, SUM(yards_gained) AS total_pass_yards
FROM {{ source('staging','pbp_data_partitioned') }}
GROUP BY posteam, game_id
