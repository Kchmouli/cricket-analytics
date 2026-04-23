with matches as (
    select * from {{ ref('stg_matches') }}
),

team_map as (
    select * from {{ ref('seed_team_name_map') }}
),

venue_map as (
    select * from {{ ref('seed_venue_map') }}
)

select
    m.match_id,
    m.match_type,
    m.year,
    m.match_date,
    m.season,
    m.gender,
    m.team_type,
    coalesce(t1.canonical_name, m.team_1)  as team_1,
    coalesce(t2.canonical_name, m.team_2)  as team_2,
    -- toss_winner and outcome_winner resolved using same canonicalisation as team_1/team_2
    case m.toss_winner
        when m.team_1 then coalesce(t1.canonical_name, m.team_1)
        when m.team_2 then coalesce(t2.canonical_name, m.team_2)
        else m.toss_winner
    end                                    as toss_winner,
    m.toss_decision,
    m.toss_uncontested,
    case m.outcome_winner
        when m.team_1 then coalesce(t1.canonical_name, m.team_1)
        when m.team_2 then coalesce(t2.canonical_name, m.team_2)
        else m.outcome_winner
    end                                    as outcome_winner,
    m.outcome_result,
    m.outcome_method,
    m.outcome_eliminator,
    m.outcome_bowl_out,
    m.outcome_by_runs,
    m.outcome_by_wickets,
    m.outcome_by_innings,
    m.player_of_match,
    m.event_name,
    m.event_match_number,
    m.event_group,
    m.event_stage,
    coalesce(v.canonical_venue, m.venue)   as venue,
    coalesce(v.canonical_city,  m.city)    as city,
    m.balls_per_over,
    m.overs,
    m.revision,

    -- convenience flags
    m.outcome_winner is not null           as has_result,
    m.outcome_result = 'tie'               as is_tie,
    m.outcome_result = 'draw'              as is_draw,
    m.outcome_result = 'no result'         as is_no_result

from matches m
left join team_map t1  on m.team_1 = t1.raw_name
left join team_map t2  on m.team_2 = t2.raw_name
left join venue_map v  on m.venue   = v.raw_venue
