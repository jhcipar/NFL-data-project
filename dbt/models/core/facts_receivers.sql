{{ config(
    materialized='table',
)}}


with game_by_game_info as (
  select game_id,season,`week`
  from {{ ref('stg_pbp_data') }}
  group by game_id,season,`week`
),

player_teams as (
  select
    receiver_player_id,
    receiver_player_name,
    posteam,
    game_id
  from {{ ref('stg_pbp_data') }}
  where receiver_player_id is not null
  group by receiver_player_id,receiver_player_name,posteam,game_id
),

rec_data_game as (
    select
        receiver_player_name,
        receiver_player_id,
        game_id,
        sum(receiving_yards) as game_rec_yards,
        count(td_player_id) as game_rec_tds,
        count(receiving_yards) as game_recs,
        count(receiver_player_id) as game_targets
    from {{ ref('stg_pbp_data') }}
    where
        receiver_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id,receiver_player_id,receiver_player_name
),

team_passes as(
    select 
        count(receiver_player_id) as team_pass_attempts,
        posteam,
        defteam,
        game_id
    from {{ ref('stg_pbp_data') }}
    where
        receiver_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id,posteam,defteam
)

select 
    rec_data_game.receiver_player_name,
    rec_data_game.receiver_player_id,
    rec_data_game.game_id,
    rec_data_game.game_rec_yards,
    rec_data_game.game_rec_tds,
    rec_data_game.game_recs,
    rec_data_game.game_targets,
    player_teams.posteam,
    game_by_game_info.season,
    game_by_game_info.`week`,
    rec_data_game.game_targets/team_passes.team_pass_attempts as target_share
from rec_data_game
left join game_by_game_info
on rec_data_game.game_id = game_by_game_info.game_id
left join player_teams
on rec_data_game.receiver_player_id = player_teams.receiver_player_id and rec_data_game.game_id = player_teams.game_id
left join team_passes
on player_teams.game_id = team_passes.game_id and player_teams.posteam = team_passes.posteam
order by target_share desc



