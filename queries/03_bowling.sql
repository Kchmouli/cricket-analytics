-- ── Bowling analysis ─────────────────────────────────────────────────────────

-- Top 20 ODI wicket takers (min 50 innings bowled)
SELECT
    player_name,
    matches,
    innings_bowled,
    total_wickets,
    bowling_avg,
    economy,
    bowling_sr,
    five_wicket_hauls
FROM main_marts.mart_bowling_career
WHERE match_type = 'odi'
  AND innings_bowled >= 50
ORDER BY total_wickets DESC
LIMIT 20;

-- Best T20 economy rates (min 50 innings, qualification)
SELECT
    player_name,
    innings_bowled,
    total_wickets,
    economy,
    bowling_avg,
    bowling_sr
FROM main_marts.mart_bowling_career
WHERE match_type = 't20'
  AND innings_bowled >= 50
ORDER BY economy
LIMIT 20;

-- Most Test five-wicket hauls
SELECT
    player_name,
    matches,
    total_wickets,
    five_wicket_hauls,
    bowling_avg,
    economy,
    first_year,
    last_year
FROM main_marts.mart_bowling_career
WHERE match_type = 'test'
ORDER BY five_wicket_hauls DESC
LIMIT 20;

-- Best bowling innings (most wickets in a single innings)
SELECT
    player_name,
    bowling_team,
    batting_team,
    match_date,
    match_type,
    wickets_credited,
    runs_conceded,
    legal_deliveries
FROM main_intermediate.int_bowling_by_innings
WHERE NOT super_over
ORDER BY wickets_credited DESC, runs_conceded
LIMIT 20;

-- Wicket types breakdown across all deliveries
SELECT
    wicket_kind,
    count(*) AS wickets
FROM main_marts.fct_deliveries
WHERE is_wicket
GROUP BY wicket_kind
ORDER BY wickets DESC;

-- Economy by over number in T20s (powerplay vs middle vs death)
SELECT
    over_number,
    count(*)                                                       AS balls,
    sum(runs_total)                                                AS runs,
    round(sum(runs_total) * 6.0 / nullif(
        count(*) - sum(extras_wides::integer), 0), 2)             AS economy
FROM main_marts.fct_deliveries
WHERE match_type = 't20'
  AND innings_number IN (1, 2)   -- exclude super overs
GROUP BY over_number
ORDER BY over_number;
