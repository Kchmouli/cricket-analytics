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
    b.batter                                                as player_name,
    b.batting_team,
    b.bowling_team,

    count(*)                                                as balls_faced,
    sum(b.runs_batter)                                      as runs,
    sum(case when b.extras_wides > 0 then 1 else 0 end)    as wides_faced,
    sum(case when b.runs_batter = 4
              and not b.runs_non_boundary then 1 else 0 end) as fours,
    sum(case when b.runs_batter = 6
              and not b.runs_non_boundary then 1 else 0 end) as sixes,
    sum(case when b.is_wicket
              and b.wicket_player_out = b.batter then 1 else 0 end) as dismissals,

    max(case when b.is_wicket
              and b.wicket_player_out = b.batter
             then b.wicket_kind end)                        as dismissal_kind,

    -- strike rate: runs per 100 legal deliveries
    round(
        sum(b.runs_batter) * 100.0
        / nullif(count(*) - sum(case when b.extras_wides > 0 then 1 else 0 end), 0),
        2
    )                                                       as strike_rate,

    -- not out flag
    sum(case when b.is_wicket
              and b.wicket_player_out = b.batter then 1 else 0 end) = 0
                                                            as not_out,
    i.super_over

from ball_by_ball b
inner join innings i
    on b.match_id = i.match_id
    and b.innings_number = i.innings_number
group by
    b.match_id, b.match_type, b.year, b.match_date, b.season,
    b.innings_number, b.batter, b.batting_team, b.bowling_team, i.super_over
