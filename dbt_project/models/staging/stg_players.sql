with source as (
    select * from {{ source('silver', 'players') }}
)

select
    match_id,
    match_type,
    year,
    player_name,
    cricsheet_id,
    team
from source
