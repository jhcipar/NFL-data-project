{{ config(
    materialized='table',
)}}


with game_by_game_info as (
  select game_id,season,`week`
  from {{ ref('stg_pbp_data') }}
  group by game_id,season,`week`
),

team_game_data as (
    select *
    from {{ ref('stg_team_game_by_game_data') }}
),

roster_data as (
    select *
    from {{ ref('stg_roster_data') }}
),

receiving_player_data as (
    select
        receiver_player_name,
        receiver_player_id,
        game_id,
        posteam,
        season,
        sum(receiving_yards) as game_rec_yards,
        count(td_player_id) as game_rec_tds,
        count(receiving_yards) as game_recs,
        count(receiver_player_id) as game_targets,
        avg(epa) as avg_play_rec_epa,
        avg(yac_epa) as avg_play_rec_yac_epa,
    from {{ ref('stg_pbp_data') }}
    where
        receiver_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id, receiver_player_id, receiver_player_name,posteam, season
),

rushing_player_data as (
    select
        rusher_player_name,
        rusher_player_id,
        game_id,
        posteam,
        season,
        count(td_player_id) as game_rush_tds,
        count(rushing_yards) as game_rush_yards,
        count(rusher_player_id) as game_rush_attempts,
        avg(epa) as avg_play_rush_epa
    from {{ ref('stg_pbp_data') }}
    where
        rusher_player_id is not null
        and rush = 1
        and season_type = 'REG'
    group by game_id, rusher_player_id, rusher_player_name,posteam, season
),

passing_player_data as (
    select
        passer_player_name,
        passer_player_id,
        game_id,
        posteam,
        season,
        sum(passing_yards) as game_pass_yards,
        count(td_player_id) as game_pass_tds,
        count(passing_yards) as game_pass_yards,
        count(passer_player_id) as game_pass_attempts,
        avg(epa) as avg_pass_play_epa,
        avg(air_epa) as avg_air_epa,
        avg(cpoe) as avg_cpoe
    from {{ ref('stg_pbp_data') }}
    where
        passer_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id, passer_player_id, passer_player_name,posteam, season
),

player_stats as (
    select
        rushing_player_data.rusher_player_name as rush_player_name,
        rushing_player_data.rusher_player_id,
        rushing_player_data.game_id,
        rushing_player_data.posteam,
        rushing_player_data.season,
        rushing_player_data.game_rush_tds,
        rushing_player_data.game_rush_yards,
        rushing_player_data.game_rush_attempts,
        rushing_player_data.avg_play_rush_epa,

        receiving_player_data.game_rec_yards,

        


  rushing_player_data.game_rush_yards + receiving_player_data.game_rec_yards as total_yards
from rushing_player_data
 full join receiving_player_data
 on rushing_player_data.game_id = receiving_player_data.game_id 
  and rushing_player_data.rusher_player_id = receiving_player_data.receiver_player_id
  order by game_rush_attempts asc
)



select * from player_stats



