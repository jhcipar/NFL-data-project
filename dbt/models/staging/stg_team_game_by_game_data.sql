{{ config(materialized='view')}}

with team_passing as (
    select 
        sum(pass_attempt) as team_pass_attempts,
        count(td_player_id) as team_pass_tds,
        sum(complete_pass) as team_pass_completions,
        sum(passing_yards) as team_passing_yards,
        sum(interception) as team_ints,
        avg(epa) as team_passing_epa_play,
        posteam as team,
        defteam as opp_team,
        game_id,
        season,
        `week`
    from {{ ref('stg_pbp_data') }}
    where
        special = 0
        and pass = 1
        and season_type = 'REG'
    group by game_id,season,`week`,posteam,defteam
),

team_rushing as(
    select 
        sum(rush) as team_rush_attempts,
        count(td_player_id) as team_rush_tds,
        sum(rushing_yards) as team_rushing_yards,
        avg(epa) as team_rushing_epa_play,
        posteam,
        defteam,
        game_id
    from {{ ref('stg_pbp_data') }}
    where
        rush = 1
        and season_type = 'REG' 
        and posteam is not null
        and special = 0
    group by game_id,posteam,defteam
),

team_fumbles as (
    select
        sum(fumble) as team_fumbles,
        sum(fumble_lost) as team_fumbles_lost,
        game_id,
        posteam,
        defteam
    from {{ ref('stg_pbp_data') }}
    where
        season_type = 'REG' and posteam is not null
    group by game_id,posteam,defteam
),

team_qb_pain as (
    select
        sum(sack) as team_sacked,
        sum(qb_hit) as team_qb_hits,
        game_id,
        posteam,
        defteam,
    from {{ ref('stg_pbp_data') }}
        where season_type = 'REG' and posteam is not null
    group by game_id,posteam,defteam
),

team_data_all as (
    select 
        team_passing.*,

        team_passing_a.team_pass_attempts    opp_team_pass_attempts,
        team_passing_a.team_pass_tds         opp_team_pass_tds,
        team_passing_a.team_pass_completions opp_team_pass_completions,
        team_passing_a.team_passing_yards    opp_team_passing_yards,
        team_passing_a.team_ints             opp_team_ints,
        team_passing_a.team_passing_epa_play opp_team_passing_epa_play,

        team_rushing.team_rush_attempts      team_rush_attempts,
        team_rushing.team_rush_tds           team_rush_tds,
        team_rushing.team_rushing_yards      team_rushing_yards,
        team_rushing.team_rushing_epa_play   team_rushing_epa_play,

        team_rushing_a.team_rush_attempts    opp_team_rush_attempts,
        team_rushing_a.team_rush_tds         opp_team_rush_tds,
        team_rushing_a.team_rushing_yards    opp_team_rushing_yards,
        team_rushing_a.team_rushing_epa_play opp_team_rushing_epa_play,

        team_fumbles.team_fumbles            team_fumbles,
        team_fumbles.team_fumbles_lost       team_fumbles_lost,

        team_qb_pain.team_sacked             team_sacked,
        team_qb_pain.team_qb_hits            team_qb_hit,

        team_qb_pain_a.team_sacked           opp_team_sacked,
        team_qb_pain_a.team_qb_hits          opp_team_qb_hit,   


    from team_passing
    left join team_passing team_passing_a
        on team_passing.game_id = team_passing_a.game_id and team_passing.opp_team = team_passing_a.team
    left join team_rushing
        on team_passing.game_id = team_rushing.game_id and team_passing.team = team_rushing.posteam
    left join team_rushing team_rushing_a
        on team_passing.game_id = team_rushing_a.game_id and team_passing.opp_team = team_rushing_a.posteam
    left join team_fumbles
        on team_passing.game_id = team_fumbles.game_id and team_passing.team = team_fumbles.posteam
    left join team_qb_pain
        on team_passing.game_id = team_qb_pain.game_id and team_passing.team = team_qb_pain.posteam
    left join team_qb_pain team_qb_pain_a
        on team_passing.game_id = team_qb_pain_a.game_id and team_passing.opp_team = team_qb_pain_a.posteam
    order by game_id
    -- order by season, `week` desc

),

final as (
    select
        team_data_all.*
    from team_data_all
)

select * from final