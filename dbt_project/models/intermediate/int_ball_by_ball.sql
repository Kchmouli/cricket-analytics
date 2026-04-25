{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['match_id', 'innings_number', 'over_number', 'ball_in_over']},
      {'columns': ['batter']},
      {'columns': ['bowler']},
    ]
  )
}}

with deliveries as (
    select * from {{ ref('stg_deliveries') }}
),

excluded_matches as (
    select match_id from {{ ref('seed_excluded_matches') }}
),

matches as (
    select
        match_id,
        match_date,
        season,
        gender,
        team_type,
        team_1,
        team_2,
        venue,
        city,
        outcome_winner,
        outcome_result,
        outcome_method,
        balls_per_over
    from {{ ref('stg_matches') }}
),

-- Actual batting team per innings from the source data.
-- info.teams[] order does not guarantee which team bats first;
-- innings[].team is the authoritative source for batting order.
innings_meta as (
    select match_id, innings_number, batting_team
    from {{ ref('stg_innings') }}
),

team_map as (
    select * from {{ ref('seed_team_name_map') }}
),

venue_map as (
    select * from {{ ref('seed_venue_map') }}

),

enriched as (
    select
        d.match_id,
        d.match_type,
        d.year,
        m.match_date,
        m.season,
        m.gender,
        m.team_type,
        d.innings_number,
        d.over_number,
        d.ball_in_over,
        d.batter,
        d.bowler,
        d.non_striker,

        -- batting team from actual innings metadata, not inferred from team_1/team_2 order
        coalesce(tb.canonical_name, i.batting_team)              as batting_team,
        -- bowling team is whichever match team is not batting
        case
            when i.batting_team = m.team_1
                then coalesce(t2.canonical_name, m.team_2)
            else coalesce(t1.canonical_name, m.team_1)
        end                                                       as bowling_team,

        coalesce(v.canonical_venue, m.venue)         as venue,
        coalesce(v.canonical_city,  m.city)          as city,

        d.runs_batter,
        d.runs_extras,
        d.runs_total,
        d.runs_non_boundary,
        d.extras_wides,
        d.extras_noballs,
        d.extras_byes,
        d.extras_legbyes,
        d.extras_penalty,
        d.wicket_count,
        d.is_wicket,
        d.wicket_kind,
        d.wicket_player_out,
        d.wicket_fielders,
        d.review_by,
        d.review_decision,
        d.review_umpires_call,

        m.outcome_winner,
        m.outcome_result,
        m.outcome_method,
        m.balls_per_over

    from deliveries d
    inner join matches m on d.match_id = m.match_id
    left join excluded_matches ex on d.match_id = ex.match_id
    inner join innings_meta i
        on  d.match_id       = i.match_id
        and d.innings_number = i.innings_number
    left join team_map tb on i.batting_team = tb.raw_name  -- canonicalize the innings batting team
    left join team_map t1 on m.team_1       = t1.raw_name  -- needed for bowling_team derivation
    left join team_map t2 on m.team_2       = t2.raw_name  -- needed for bowling_team derivation
    left join venue_map v  on m.venue       = v.raw_venue
    where ex.match_id is null
)

select * from enriched
