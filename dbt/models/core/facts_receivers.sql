{{ config(
    materialized='table',
)}}


with rec_data_game as (
    select
        max(season) as season,
        avg(`week`) as `week`,
        max(receiver_player_name) as receiver_player_name,
        max(receiver_player_id) as receiver_id,
        max(game_id) as game,
        max(posteam) as posteam,
        max(defteam) as defteam,
        sum(receiving_yards) as game_rec_yards,
        count(td_player_id) as game_rec_tds,
        count(receiving_yards) as game_recs,
        count(receiver_player_id) as game_targets,
    from {{ ref('stg_pbp_data') }}
    where
        receiver_player_name is not null
        and pass = 1
        and season_type = 'REG'
    group by game_id,receiver_player_id
    order by season,`week` asc
),


injury_info as (
   select
        max(season) as injury_season,
        max(team) as injury_team,
        max(`week`) as injury_week,
        max(position) as position,
        max(report_status) as report_status,
        max(off_def) as off_def,
        count(1) as def_weekly_injuries
    from {{ ref('stg_injury_data') }}
    where off_def = 'DEF' and report_status = 'Out'
    group by season,`week`,team
)

select 
    rec_data_game.season,
    rec_data_game.`week`,
    rec_data_game.receiver_player_name,
    rec_data_game.receiver_id,
    rec_data_game.game,
    rec_data_game.posteam,
    rec_data_game.defteam,
    rec_data_game.game_rec_yards,
    rec_data_game.game_rec_tds,
    rec_data_game.game_recs,
    rec_data_game.game_targets,
    avg(rec_data_game.game_rec_yards) over (partition by rec_data_game.receiver_id,rec_data_game.season) as season_average,
    sum(rec_data_game.game_targets) over (partition by rec_data_game.game,rec_data_game.posteam) as team_total_passes,
    injury_info.def_weekly_injuries
from rec_data_game
left join injury_info
on rec_data_game.`week` = injury_info.injury_week and rec_data_game.season = injury_info.injury_season and rec_data_game.defteam = injury_info.injury_team
order by rec_data_game.game_rec_tds asc



