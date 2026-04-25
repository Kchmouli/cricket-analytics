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
),

-- Players dismissed without facing a ball (e.g. run out as non-striker).
-- They appear in all_dismissals but not in batting, so must be added
-- explicitly to avoid undercounting innings and inflating averages.
zero_ball_dismissals as (
    select distinct
        d.match_id,
        b.match_type,
        b.year,
        b.match_date,
        b.season,
        d.innings_number,
        d.player_name,
        b.batting_team,
        b.bowling_team,
        0                               as balls_faced,
        0                               as runs,
        0                               as wides_faced,
        0                               as fours,
        0                               as sixes,
        cast(null as double)            as strike_rate,
        i.super_over
    from all_dismissals d
    inner join ball_by_ball b
        on  d.match_id       = b.match_id
        and d.innings_number = b.innings_number
        and b.is_wicket
        and b.wicket_player_out = d.player_name
    inner join innings i
        on  d.match_id       = i.match_id
        and d.innings_number = i.innings_number
    where not exists (
        select 1 from batting bt
        where  bt.match_id       = d.match_id
          and  bt.innings_number  = d.innings_number
          and  bt.player_name     = d.player_name
    )
),

-- Players who reached the crease as non-striker but faced 0 balls,
-- were not dismissed, and the innings ended before they could bat.
-- They appear in neither batting nor all_dismissals.
non_striker_only as (
    select distinct
        b.match_id,
        b.match_type,
        b.year,
        b.match_date,
        b.season,
        b.innings_number,
        b.non_striker                   as player_name,
        b.batting_team,
        b.bowling_team,
        0                               as balls_faced,
        0                               as runs,
        0                               as wides_faced,
        0                               as fours,
        0                               as sixes,
        cast(null as double)            as strike_rate,
        i.super_over
    from ball_by_ball b
    inner join innings i
        on  b.match_id       = i.match_id
        and b.innings_number = i.innings_number
    where not exists (
        select 1 from batting bt
        where  bt.match_id       = b.match_id
          and  bt.innings_number  = b.innings_number
          and  bt.player_name     = b.non_striker
    )
    and not exists (
        select 1 from all_dismissals d
        where  d.match_id       = b.match_id
          and  d.innings_number  = b.innings_number
          and  d.player_name     = b.non_striker
    )
    -- Exclude if the player bats in any innings of this match.
    -- Prevents old Cricsheet files where a player's name appears as non_striker
    -- in the opposing team's innings from creating a spurious extra innings.
    and not exists (
        select 1 from batting bt2
        where  bt2.match_id    = b.match_id
          and  bt2.player_name = b.non_striker
    )
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

union all

select
    z.*,
    1                                                           as dismissals,
    d.dismissal_kind,
    false                                                       as not_out
from zero_ball_dismissals z
inner join all_dismissals d
    on  z.match_id       = d.match_id
    and z.innings_number  = d.innings_number
    and z.player_name     = d.player_name

union all

select
    n.*,
    0                                                           as dismissals,
    cast(null as varchar)                                       as dismissal_kind,
    true                                                        as not_out
from non_striker_only n
