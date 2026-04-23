with deliveries as (
    select * from {{ ref('stg_deliveries') }}
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

        -- batting team resolved from innings join via matches
        case
            when d.innings_number % 2 = 1 then coalesce(t1.canonical_name, m.team_1)
            else coalesce(t2.canonical_name, m.team_2)
        end                                          as batting_team,
        case
            when d.innings_number % 2 = 1 then coalesce(t2.canonical_name, m.team_2)
            else coalesce(t1.canonical_name, m.team_1)
        end                                          as bowling_team,

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
    left join team_map t1 on m.team_1 = t1.raw_name
    left join team_map t2 on m.team_2 = t2.raw_name
    left join venue_map v  on m.venue  = v.raw_venue
)

select * from enriched
