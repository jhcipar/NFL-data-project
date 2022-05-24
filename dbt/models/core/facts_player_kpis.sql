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
        receiver_player_name as player_name,
        receiver_player_id as player_id,
        game_id,
        posteam,
        season,
        `week`,
        sum(receiving_yards) as game_rec_yards,
        count(td_player_id) as game_rec_tds,
        sum(complete_pass) as game_recs,
        count(receiver_player_id) as game_targets,
        avg(epa) as avg_play_rec_epa,
        avg(yac_epa) as avg_play_rec_yac_epa,
        sum(fumble_lost) as rec_fumbles_lost,
    from {{ ref('stg_pbp_data') }}
    where
        receiver_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id, receiver_player_id, receiver_player_name,posteam, season,`week`
),

rushing_player_data as (
    select
        rusher_player_name as player_name,
        rusher_player_id as player_id,
        game_id,
        posteam,
        season,
        `week`,
        count(td_player_id) as game_rush_tds,
        sum(rushing_yards) as game_rush_yards,
        count(rusher_player_id) as game_rush_attempts,
        avg(epa) as avg_play_rush_epa,
        sum(fumble_lost) as rush_fumbles_lost,
    from {{ ref('stg_pbp_data') }}
    where
        rusher_player_id is not null
        and rush = 1
        and season_type = 'REG'
    group by game_id, rusher_player_id, rusher_player_name,posteam, season, `week`
),

passing_player_data as (
    select
        passer_player_name as player_name,
        passer_player_id as player_id,
        game_id,
        posteam,
        season,
        `week`,
        sum(passing_yards) as game_pass_yards,
        count(td_player_id) as game_pass_tds,
        count(passer_player_id) as game_pass_attempts,
        sum(complete_pass) as game_pass_completions,
        sum(interception) as game_ints,
        avg(epa) as avg_pass_play_epa,
        avg(air_epa) as avg_air_epa,
        avg(cpoe) as avg_cpoe,
        sum(fumble_lost) as pass_fumbles_lost,
    from {{ ref('stg_pbp_data') }}
    where
        passer_player_id is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id, passer_player_id, passer_player_name,posteam, season,`week`
),

player_stats as (
    select
        coalesce(rushing_player_data.player_name,passing_player_data.player_name,receiving_player_data.player_name) as player_name,
        coalesce(rushing_player_data.player_id,passing_player_data.player_id,receiving_player_data.player_id) as player_id,
        coalesce(rushing_player_data.game_id,passing_player_data.game_id,receiving_player_data.game_id) as game_id,
        coalesce(rushing_player_data.posteam,passing_player_data.posteam,receiving_player_data.posteam) as team,
        coalesce(rushing_player_data.season,passing_player_data.season,receiving_player_data.season) as season,
        coalesce(rushing_player_data.`week`,passing_player_data.`week`,receiving_player_data.`week`) as `week`,

        rushing_player_data.game_rush_tds,
        rushing_player_data.game_rush_yards,
        rushing_player_data.game_rush_attempts,
        rushing_player_data.avg_play_rush_epa,
        rushing_player_data.rush_fumbles_lost,

        receiving_player_data.game_rec_yards,
        receiving_player_data.game_rec_tds,
        receiving_player_data.game_recs,
        receiving_player_data.game_targets,
        receiving_player_data.avg_play_rec_epa,
        receiving_player_data.avg_play_rec_yac_epa,
        receiving_player_data.rec_fumbles_lost,

        passing_player_data.game_pass_yards,
        passing_player_data.game_pass_tds,
        passing_player_data.game_pass_attempts,
        passing_player_data.game_pass_completions,
        passing_player_data.avg_pass_play_epa,
        passing_player_data.avg_air_epa,
        passing_player_data.avg_cpoe,
        passing_player_data.game_ints,
        passing_player_data.pass_fumbles_lost,
        
from rushing_player_data
full join receiving_player_data
    on rushing_player_data.game_id = receiving_player_data.game_id 
        and rushing_player_data.player_id = receiving_player_data.player_id
full join passing_player_data
    on rushing_player_data.game_id = passing_player_data.game_id
        and rushing_player_data.player_id = passing_player_data.player_id

  order by game_id
),

final as (
    select 
        ifnull(game_pass_yards,0) / 25 + 
        ifnull(game_pass_tds,0) * 4 +
        ifnull(game_ints, 0) * -2 +
        ifnull(game_rush_tds, 0) * 6 +
        ifnull(game_rush_yards, 0) / 10 +
        ifnull(game_recs, 0) +
        ifnull(game_rec_yards, 0) / 10 +
        ifnull(game_rec_tds, 0) * 6 +
        ifnull(rec_fumbles_lost, 0) * -2 +
        ifnull(pass_fumbles_lost, 0) * -2 +
        ifnull(rush_fumbles_lost, 0) * -2 as fantasy_points_ppr,

        player_stats.*, 

        roster_data.position,

        team_game_data.opp_team,

        player_stats.game_targets / team_game_data.team_pass_attempts as rec_target_share,
        player_stats.game_rush_attempts / team_game_data.team_rush_attempts as rush_attempt_share,

    
    from player_stats
    left join roster_data
        on player_stats.player_id = roster_data.gsis_id and
        player_stats.season = roster_data.season
    left join team_game_data
        on player_stats.team = team_game_data.team
        and player_stats.game_id = team_game_data.game_id
    order by fantasy_points_ppr desc
)

select * from final




