with source as (
    select * from {{ source('silver', 'deliveries') }}
)

select
    match_id,
    match_type,
    year,
    innings_number,
    over_number,
    ball_in_over,
    batter,
    bowler,
    non_striker,
    runs_batter,
    runs_extras,
    runs_total,
    coalesce(runs_non_boundary, false) as runs_non_boundary,
    extras_wides,
    extras_noballs,
    extras_byes,
    extras_legbyes,
    extras_penalty,
    wicket_count,
    is_wicket,
    wicket_kind,
    wicket_player_out,
    wicket_fielders,
    review_by,
    review_decision,
    review_umpires_call
from source
