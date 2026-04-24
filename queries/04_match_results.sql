-- ── Match and team analysis ───────────────────────────────────────────────────

-- Win/loss record for a team in ODIs
SELECT
    team_1                                  AS team,
    count(*)                                AS played,
    sum((outcome_winner = team_1)::integer) AS won,
    sum((outcome_winner = team_2)::integer) AS lost,
    sum(is_tie::integer)                    AS tied,
    sum((NOT has_result)::integer)          AS no_result,
    round(
        sum((outcome_winner = team_1)::integer) * 100.0
        / nullif(sum(has_result::integer), 0), 1)  AS win_pct
FROM main_marts.fct_match_results
WHERE match_type = 'odi'
  AND (team_1 = 'India' OR team_2 = 'India')
  AND (team_1 = 'India')   -- flip to see from opponent side

UNION ALL

SELECT
    team_2,
    count(*),
    sum((outcome_winner = team_2)::integer),
    sum((outcome_winner = team_1)::integer),
    sum(is_tie::integer),
    sum((NOT has_result)::integer),
    round(sum((outcome_winner = team_2)::integer) * 100.0
        / nullif(sum(has_result::integer), 0), 1)
FROM main_marts.fct_match_results
WHERE match_type = 'odi'
  AND team_2 = 'India';

-- Head-to-head record between two teams (all formats)
SELECT
    match_type,
    count(*)                                    AS played,
    sum((outcome_winner = 'India')::integer)    AS india_wins,
    sum((outcome_winner = 'Australia')::integer) AS aus_wins,
    sum(is_tie::integer)                        AS ties
FROM main_marts.fct_match_results
WHERE has_result
  AND (
    (team_1 = 'India' AND team_2 = 'Australia') OR
    (team_1 = 'Australia' AND team_2 = 'India')
  )
GROUP BY match_type
ORDER BY match_type;

-- Toss advantage: does winning the toss help?
SELECT
    match_type,
    toss_decision,
    count(*)                                                        AS matches,
    sum((toss_winner = outcome_winner)::integer)                    AS toss_winner_won,
    round(
        sum((toss_winner = outcome_winner)::integer) * 100.0
        / nullif(count(*), 0), 1)                                   AS toss_win_pct
FROM main_marts.fct_match_results
WHERE has_result
  AND match_type IN ('t20', 'odi', 'test')
GROUP BY match_type, toss_decision
ORDER BY match_type, toss_decision;

-- Biggest winning margins (runs) in T20s
SELECT
    team_1,
    team_2,
    outcome_winner,
    outcome_by_runs,
    match_date,
    venue
FROM main_marts.fct_match_results
WHERE match_type = 't20'
  AND outcome_by_runs IS NOT NULL
ORDER BY outcome_by_runs DESC
LIMIT 20;

-- Most player of the match awards
SELECT
    player_of_match    AS player,
    match_type,
    count(*)           AS awards
FROM main_marts.fct_match_results
WHERE player_of_match IS NOT NULL
GROUP BY player_of_match, match_type
ORDER BY awards DESC
LIMIT 20;

-- Matches at a specific venue
SELECT
    venue,
    city,
    match_type,
    count(*)           AS matches,
    min(match_date)    AS first_match,
    max(match_date)    AS last_match
FROM main_marts.fct_match_results
GROUP BY venue, city, match_type
ORDER BY matches DESC
LIMIT 30;
