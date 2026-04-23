with innings as (
    select * from {{ ref('int_bowling_by_innings') }}
    where not super_over  -- exclude super overs from career stats
)

select
    player_name,
    match_type,

    count(distinct match_id)                                as matches,
    count(*)                                                as innings_bowled,
    sum(legal_deliveries)                                   as total_balls,
    sum(runs_conceded)                                      as total_runs,
    sum(wickets_credited)                                   as total_wickets,
    sum(no_balls)                                           as total_no_balls,
    sum(wides)                                              as total_wides,

    -- best bowling in an innings (most wickets, then fewest runs)
    max(wickets_credited)                                   as best_wickets,

    -- bowling average: runs per wicket
    round(
        sum(runs_conceded)::double
        / nullif(sum(wickets_credited), 0),
        2
    )                                                       as bowling_avg,

    -- economy: runs per over
    round(
        sum(runs_conceded)::double
        / nullif(sum(legal_deliveries), 0)
        * 6,
        2
    )                                                       as economy,

    -- bowling strike rate: balls per wicket
    round(
        sum(legal_deliveries)::double
        / nullif(sum(wickets_credited), 0),
        2
    )                                                       as bowling_sr,

    -- wicket hauls
    sum(case when wickets_credited >= 5 then 1 else 0 end) as five_wicket_hauls,
    sum(case when wickets_credited >= 4 then 1 else 0 end) as four_plus_wicket_hauls,

    min(year)                                               as first_year,
    max(year)                                               as last_year

from innings
group by player_name, match_type
