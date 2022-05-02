{{ config(materialized='view')}}

select 
*,
{{ convert_pos_offense_defense('position') }} as off_def
FROM {{ source('staging','external_injury_data') }}