with source as (
    select * from {{ source('silver', 'innings') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by match_id, innings_number order by match_id) = 1
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
from deduped
