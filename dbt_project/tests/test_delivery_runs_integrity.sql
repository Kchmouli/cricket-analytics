-- Every delivery: runs_total must equal runs_batter + runs_extras
select
    match_id,
    innings_number,
    over_number,
    ball_in_over,
    runs_batter,
    runs_extras,
    runs_total
from {{ ref('stg_deliveries') }}
where runs_total != runs_batter + runs_extras
