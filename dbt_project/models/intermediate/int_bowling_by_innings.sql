with ball_by_ball as (
    select * from {{ ref('int_ball_by_ball') }}
),

innings as (
    select * from {{ ref('stg_innings') }}
)

select
    b.match_id,
    b.match_type,
    b.year,
    b.match_date,
    b.season,
    b.innings_number,
    b.bowler                                                    as player_name,
    b.bowling_team,
    b.batting_team,

    -- legal deliveries (exclude wides and no-balls for over count)
    sum(case when b.extras_wides = 0
              and b.extras_noballs = 0 then 1 else 0 end)       as legal_deliveries,
    count(*)                                                    as total_deliveries,

    -- overs bowled (complete overs + remaining balls as decimal)
    floor(
        sum(case when b.extras_wides = 0
                  and b.extras_noballs = 0 then 1 else 0 end)
        / b.balls_per_over
    )
    + (
        sum(case when b.extras_wides = 0
                  and b.extras_noballs = 0 then 1 else 0 end)
        % b.balls_per_over
    ) * 0.1                                                     as overs_bowled,

    sum(b.runs_total)                                           as runs_conceded,
    sum(b.wicket_count)                                         as wickets,

    -- exclude run-outs from bowler's wicket count
    sum(case when b.is_wicket
              and b.wicket_kind not in ('run out', 'retired hurt', 'retired out',
                                        'obstructing the field')
             then 1 else 0 end)                                 as wickets_credited,

    sum(case when b.extras_noballs > 0 then 1 else 0 end)      as no_balls,
    sum(case when b.extras_wides   > 0 then 1 else 0 end)      as wides,

    -- economy: runs per over
    round(
        sum(b.runs_total) * cast(b.balls_per_over as double)
        / nullif(sum(case when b.extras_wides = 0
                           and b.extras_noballs = 0 then 1 else 0 end), 0),
        2
    )                                                           as economy,

    i.super_over

from ball_by_ball b
inner join innings i
    on b.match_id = i.match_id
    and b.innings_number = i.innings_number
group by
    b.match_id, b.match_type, b.year, b.match_date, b.season,
    b.innings_number, b.bowler, b.bowling_team, b.batting_team,
    b.balls_per_over, i.super_over
