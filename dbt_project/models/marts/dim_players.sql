with players as (
    select * from {{ ref('stg_players') }}
),

-- one canonical row per player: prefer the cricsheet_id that appears most
player_ids as (
    select
        player_name,
        cricsheet_id,
        count(*) as appearances,
        row_number() over (
            partition by player_name
            order by count(*) desc
        ) as rn
    from players
    where cricsheet_id is not null
    group by player_name, cricsheet_id
),

canonical_id as (
    select player_name, cricsheet_id
    from player_ids
    where rn = 1
),

stats as (
    select
        player_name,
        count(distinct match_id)                        as total_matches,
        count(distinct match_type)                      as match_types_played,
        string_agg(distinct match_type, ', '
            order by match_type)                        as match_types,
        count(distinct team)                            as teams_played_for,
        string_agg(distinct team, ', '
            order by team)                              as teams,
        min(year::integer)                              as first_year,
        max(year::integer)                              as last_year
    from players
    group by player_name
)

select
    s.player_name,
    c.cricsheet_id,
    s.total_matches,
    s.match_types_played,
    s.match_types,
    s.teams_played_for,
    s.teams,
    s.first_year,
    s.last_year
from stats s
left join canonical_id c using (player_name)
