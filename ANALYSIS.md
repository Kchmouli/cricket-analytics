# Cricket Analytics — Analysis Guide

## Getting fresh data

The nightly pipeline runs at 3 AM UTC and automatically uploads `gold.duckdb` to Azure.
To pull the latest copy locally, open a **cmd** terminal (not PowerShell) from the project root:

```
.venv\Scripts\python.exe ingestion\download_gold.py
```

Run this once before each analysis session. Takes ~30 seconds (~200 MB download).

> **Note:** Run from `C:\Users\Chandra.Mouli\cricket-analytics` — not from a subdirectory.

---

## Querying in VS Code (Jupyter Notebook)

1. Open `analysis.ipynb` in VS Code

2. Select the `.venv` Python kernel when prompted (click **Select Kernel** → **Python Environments** → choose the venv)

3. Run the first cell (imports + `duckdb.connect`) — you should see `Connected to gold.duckdb`

4. Run any query cell with **Shift+Enter** — results display as a pandas DataFrame table

5. Modify queries directly in cells — change player names, filters, date ranges as needed

---

## Gold layer tables

| Table | Grain | Use for |
|---|---|---|
| `main_marts.fct_match_results` | One row per match | Team records, match outcomes, toss analysis |
| `main_marts.mart_batting_career` | One row per player + match_type | Career batting averages, run totals, milestones |
| `main_marts.mart_bowling_career` | One row per player + match_type | Career bowling averages, wicket totals |
| `main_marts.fct_deliveries` | One row per ball | Ball-by-ball analysis, over patterns, wagon wheels |
| `main_marts.dim_players` | One row per player | Player lookup, Cricsheet UUID |
| `main_intermediate.int_batting_by_innings` | One row per player per innings | Innings-level batting (form, conditions) |
| `main_intermediate.int_bowling_by_innings` | One row per bowler per innings | Innings-level bowling |

**match_type values:** `t20`, `odi`, `test`, `it20`, `odm`, `mdm`

---

## Key columns reference

### fct_match_results
`match_id, match_type, match_date, year, season, team_1, team_2,`
`toss_winner, toss_decision, outcome_winner, outcome_result, outcome_method,`
`outcome_by_runs, outcome_by_wickets, venue, city, event_name, has_result, is_tie, is_draw`

### mart_batting_career
`player_name, match_type, matches, innings, not_outs, total_runs, highest_score,`
`batting_avg, batting_sr, centuries, fifties, ducks, total_fours, total_sixes, first_year, last_year`

### mart_bowling_career
`player_name, match_type, matches, innings_bowled, total_wickets, total_runs,`
`bowling_avg, economy, bowling_sr, five_wicket_hauls, best_wickets, first_year, last_year`

### fct_deliveries
`match_id, match_type, year, match_date, innings_number, over_number, ball_in_over,`
`batter, bowler, batting_team, bowling_team, runs_batter, runs_total, runs_non_boundary,`
`extras_wides, extras_noballs, is_wicket, wicket_kind, wicket_player_out`

---

## Sample queries

See the `queries/` folder:
- `queries/01_quick_checks.sql` — data health and coverage
- `queries/02_batting.sql` — batting analysis
- `queries/03_bowling.sql` — bowling analysis
- `queries/04_match_results.sql` — team and match analysis
