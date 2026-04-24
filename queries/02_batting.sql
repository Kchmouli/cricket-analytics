-- ── Batting analysis ─────────────────────────────────────────────────────────

-- Top 20 ODI run scorers (min 50 innings)
SELECT
    player_name,
    matches,
    innings,
    total_runs,
    highest_score,
    batting_avg,
    batting_sr,
    centuries,
    fifties
FROM main_marts.mart_batting_career
WHERE match_type = 'odi'
  AND innings >= 50
ORDER BY total_runs DESC
LIMIT 20;

-- Top T20 batters by strike rate (min 30 innings)
SELECT
    player_name,
    innings,
    total_runs,
    batting_avg,
    batting_sr,
    total_sixes,
    total_fours
FROM main_marts.mart_batting_career
WHERE match_type = 't20'
  AND innings >= 30
ORDER BY batting_sr DESC
LIMIT 20;

-- Most Test centuries
SELECT
    player_name,
    matches,
    innings,
    centuries,
    fifties,
    total_runs,
    batting_avg,
    first_year,
    last_year
FROM main_marts.mart_batting_career
WHERE match_type = 'test'
ORDER BY centuries DESC
LIMIT 20;

-- Career stats for a specific player across all formats
SELECT
    player_name,
    match_type,
    matches,
    innings,
    total_runs,
    batting_avg,
    batting_sr,
    centuries,
    fifties
FROM main_marts.mart_batting_career
WHERE player_name = 'V Kohli'
ORDER BY match_type;

-- Innings-level: highest individual scores in ODIs
SELECT
    player_name,
    batting_team,
    bowling_team,
    match_date,
    runs,
    balls_faced,
    strike_rate,
    fours,
    sixes,
    not_out
FROM main_intermediate.int_batting_by_innings
WHERE match_type = 'odi'
ORDER BY runs DESC
LIMIT 20;

-- Six-hitters: most T20 sixes in a single innings
SELECT
    player_name,
    batting_team,
    bowling_team,
    match_date,
    runs,
    sixes,
    fours,
    balls_faced
FROM main_intermediate.int_batting_by_innings
WHERE match_type = 't20'
ORDER BY sixes DESC
LIMIT 20;

-- Batting average trend by year for a player (ODIs)
SELECT
    year,
    count(distinct match_id)                                  AS matches,
    sum(runs)                                                 AS runs,
    round(sum(runs)::double / nullif(sum(dismissals), 0), 2) AS avg
FROM main_intermediate.int_batting_by_innings
WHERE player_name = 'V Kohli'
  AND match_type  = 'odi'
  AND NOT super_over
GROUP BY year
ORDER BY year;
