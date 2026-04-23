with source as (
    select * from {{ source('silver', 'matches') }}
)

select
    match_id,
    match_type,
    year,
    try_cast(match_date as date)       as match_date,
    season,
    venue,
    city,
    gender,
    team_type,
    team_1,
    team_2,
    toss_winner,
    toss_decision,
    coalesce(toss_uncontested, false)  as toss_uncontested,
    outcome_winner,
    outcome_result,
    outcome_method,
    outcome_eliminator,
    outcome_bowl_out,
    outcome_by_runs,
    outcome_by_wickets,
    outcome_by_innings,
    player_of_match,
    event_name,
    event_match_number,
    event_group,
    event_stage,
    balls_per_over,
    overs,
    match_type_number,
    revision
from source
