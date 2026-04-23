with source as (
    select * from {{ source('silver', 'innings') }}
)

select
    match_id,
    match_type,
    year,
    innings_number,
    batting_team,
    coalesce(declared,  false) as declared,
    coalesce(forfeited, false) as forfeited,
    coalesce(super_over, false) as super_over,
    target_runs,
    target_overs,
    coalesce(penalty_runs_pre,  0) as penalty_runs_pre,
    coalesce(penalty_runs_post, 0) as penalty_runs_post
from source
