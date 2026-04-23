with innings as (
    select * from {{ ref('int_batting_by_innings') }}
    where not super_over  -- exclude super overs from career stats
)

select
    player_name,
    match_type,

    count(distinct match_id)                                as matches,
    count(*)                                                as innings,
    sum(not_out::integer)                                   as not_outs,
    sum(runs)                                               as total_runs,
    max(runs)                                               as highest_score,
    sum(balls_faced)                                        as total_balls,
    sum(fours)                                              as total_fours,
    sum(sixes)                                              as total_sixes,
    sum(dismissals)                                         as total_dismissals,

    -- batting average: runs / dismissals
    round(
        sum(runs)::double
        / nullif(sum(dismissals), 0),
        2
    )                                                       as batting_avg,

    -- strike rate: runs per 100 legal balls
    round(
        sum(runs) * 100.0
        / nullif(sum(balls_faced), 0),
        2
    )                                                       as batting_sr,

    -- milestone innings
    sum(case when runs >= 100 then 1 else 0 end)            as centuries,
    sum(case when runs >= 50
              and runs < 100 then 1 else 0 end)             as fifties,
    sum(case when runs = 0
              and not not_out then 1 else 0 end)            as ducks,

    min(year)                                               as first_year,
    max(year)                                               as last_year

from innings
group by player_name, match_type
