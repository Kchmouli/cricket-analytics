{{ config(materialized = 'view') }}

select * from {{ ref('int_ball_by_ball') }}
