-- ── Data coverage ────────────────────────────────────────────────────────────

-- Matches by type
SELECT match_type, count(*) AS matches
FROM main_marts.fct_match_results
GROUP BY match_type
ORDER BY matches DESC;

-- Matches per year (last 10 years)
SELECT year, match_type, count(*) AS matches
FROM main_marts.fct_match_results
WHERE year >= year(today()) - 10
GROUP BY year, match_type
ORDER BY year DESC, matches DESC;

-- Most recent matches loaded
SELECT match_id, match_type, match_date, team_1, team_2, outcome_winner
FROM main_marts.fct_match_results
ORDER BY match_date DESC
LIMIT 20;

-- Total deliveries in the dataset
SELECT match_type, count(*) AS deliveries
FROM main_marts.fct_deliveries
GROUP BY match_type
ORDER BY deliveries DESC;
