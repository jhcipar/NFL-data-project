{{ config(materialized='view')}}

select 
    season,
    team,
    position,
    `status`,
    full_name,
    first_name,
    last_name,
    birth_date,
    height,
    college,
    high_school,
    gsis_id,
    espn_id,
    sportradar_id,
    yahoo_id,
    rotowire_id,
    pff_id,
    pfr_id,
    fantasy_data_id,
    years_exp,
    headshot_url
FROM {{ source('staging','external_roster_data') }}