{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['match_id', 'innings_number', 'over_number', 'ball_in_over']},
      {'columns': ['batter']},
      {'columns': ['bowler']},
    ]
  )
}}

select * from {{ ref('int_ball_by_ball') }}
