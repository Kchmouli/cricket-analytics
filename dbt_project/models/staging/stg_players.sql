with source as (
    select * from {{ source('silver', 'players') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by match_id, player_name, team order by match_id) = 1
)

select
    match_id,
    match_type,
    year,
    player_name,
    cricsheet_id,
    team
from deduped
