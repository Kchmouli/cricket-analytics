-- When a match has a winner, that team must be one of the two participants
select
    match_id,
    team_1,
    team_2,
    outcome_winner
from {{ ref('fct_match_results') }}
where outcome_winner is not null
  and outcome_winner != team_1
  and outcome_winner != team_2
