with ball_by_ball as (
    select * from {{ ref('int_ball_by_ball') }}
),

innings as (
    select * from {{ ref('stg_innings') }}
),

-- All dismissals across every delivery in an innings.
-- Checks wicket_player_out regardless of who is batter, so run outs of
-- the non-striker are correctly attributed to the dismissed player.
all_dismissals as (
    select
        match_id,
        innings_number,
        wicket_player_out                as player_name,
        max(wicket_kind)                 as dismissal_kind
    from ball_by_ball
    where is_wicket and wicket_player_out is not null
    group by match_id, innings_number, wicket_player_out
),

batting as (
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

        -- strike rate: runs per 100 legal deliveries
        round(
            sum(b.runs_batter) * 100.0
            / nullif(count(*) - sum(case when b.extras_wides > 0 then 1 else 0 end), 0),
            2
        )                                                       as strike_rate,

        i.super_over

    from ball_by_ball b
    inner join innings i
        on b.match_id = i.match_id
        and b.innings_number = i.innings_number
    group by
        b.match_id, b.match_type, b.year, b.match_date, b.season,
        b.innings_number, b.batter, b.batting_team, b.bowling_team, i.super_over
)

select
    bt.*,
    case when d.player_name is not null then 1 else 0 end       as dismissals,
    d.dismissal_kind,
    (d.player_name is null)                                     as not_out
from batting bt
left join all_dismissals d
    on  bt.match_id      = d.match_id
    and bt.innings_number = d.innings_number
    and bt.player_name    = d.player_name
