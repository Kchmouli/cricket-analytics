# Cricket Analytics Platform

## Stack
- Python 3.13, Windows native cmd
- DuckDB 1.5.2 for ingestion and SQL queries
- PyArrow 24.0.0 for Parquet writing in silver transformer
- dbt-duckdb 1.10.1 (installed via pipx) for transformation
- ADLS Gen2 for storage (Bronze / Silver / Gold)
- GitHub Actions for nightly batch orchestration

## Shell
- Always use cmd syntax (not PowerShell, not bash)
- dbt is at %USERPROFILE%\.local\bin\dbt.exe
- venv activation: .venv\Scripts\activate.bat

## Data Sources
- Cricsheet JSON: https://cricsheet.org/downloads/
- Full load URL: https://cricsheet.org/downloads/all_male_json.zip
- Incremental URL: https://cricsheet.org/downloads/recently_added_2_male_json.zip
- Registry: https://cricsheet.org/register/

## Cricsheet JSON Format
- meta.revision increments when a file is updated (filename stays same)
- meta.created is the original creation date
- info.match_type values: T20, ODI, Test, IT20, ODM, MDM (mixed case)
- info.dates is always an array, first element used for year partitioning
- info.registry.people maps player names to Cricsheet UUIDs

## Azure Storage
- Account: cricketanalyticsdata
- Container: cricket-data
- Bronze: raw JSON converted to Parquet, partitioned by match_type and year
- Silver: normalized tables (matches, deliveries, innings, players)
- Gold: dbt mart models

## ADLS Paths (from .env)
BRONZE_PATH=abfs://cricket-data@cricketanalyticsdata.dfs.core.windows.net/bronze
SILVER_PATH=abfs://cricket-data@cricketanalyticsdata.dfs.core.windows.net/silver
GOLD_PATH=abfs://cricket-data@cricketanalyticsdata.dfs.core.windows.net/gold

## Folder Structure
- ingestion/          Bronze + Silver pipeline scripts
- enrichment/         Phase 2 player enrichment
- dbt_project/        All dbt models seeds tests
- .github/workflows/  GitHub Actions

## GitHub Actions Workflows
- `.github/workflows/nightly.yml`   — runs 3 AM UTC daily (incremental)
- `.github/workflows/full_load.yml` — manual only, requires typing "FULL" to confirm

### Nightly pipeline steps
1. `download.py incremental`            → data/raw/ (new JSON only)
2. `bronze_loader.py`                   → ADLS bronze/
3. `silver_transformer.py --mode incremental --since-days 2` → data/silver/
4. `silver_loader.py`                   → ADLS silver/
5. `dbt seed + run + test --target prod` → data/gold.duckdb

### GitHub Secrets required
- AZURE_STORAGE_ACCOUNT_NAME
- AZURE_STORAGE_ACCOUNT_KEY
- AZURE_STORAGE_CONTAINER

### dbt prod vs dev
- dev:  silver reads from ../data/silver (local Parquet)
- prod: silver reads from az://cricket-data/silver (ADLS via DuckDB azure extension)
- Both write gold to ../data/gold.duckdb
- Pass silver path for prod: --vars '{"silver_path": "az://cricket-data/silver"}'

## Conventions
- Parquet partitioned by match_type and year
- dbt naming: stg_ / int_ / fct_ / dim_
- Secrets in .env only never committed
- Revision-aware uploads: store revision as ADLS metadata
  skip upload if remote revision matches local

## Phase 1 Build Order
1. Azure infrastructure                    DONE
2. Bronze ingestion                        DONE
3. Silver transformation (PyArrow)         DONE
4. dbt scaffold + staging models           DONE
5. dbt intermediate + mart models          DONE
6. dbt tests + docs                        DONE
7. GitHub Actions nightly pipeline         DONE

## Phase 2
8. Player enrichment + entity resolution

## Silver Layer Schema
Source: ingestion/silver_transformer.py — 4 tables, all partitioned by match_type + year

Parquet written via pyarrow write_to_dataset. Partition columns (match_type, year) are
stored in the Hive directory path, not inside the Parquet files. Read with DuckDB using
hive_partitioning=true or via dbt source with dbt-duckdb ADLS config.

### matches (31 cols)
match_id, match_type, year, match_date, season, venue, city, gender, team_type,
team_1, team_2, toss_winner, toss_decision, toss_uncontested,
outcome_winner, outcome_result, outcome_method, outcome_eliminator, outcome_bowl_out,
outcome_by_runs, outcome_by_wickets, outcome_by_innings,
player_of_match, event_name, event_match_number, event_group, event_stage,
balls_per_over, overs, match_type_number, revision

### innings (12 cols)
match_id, match_type, year, innings_number, batting_team,
declared, forfeited, super_over,
target_runs, target_overs, penalty_runs_pre, penalty_runs_post

### deliveries (26 cols)
match_id, match_type, year, innings_number, over_number, ball_in_over,
batter, bowler, non_striker,
runs_batter, runs_extras, runs_total, runs_non_boundary,
extras_wides, extras_noballs, extras_byes, extras_legbyes, extras_penalty,
wicket_count, is_wicket, wicket_kind, wicket_player_out, wicket_fielders,
review_by, review_decision, review_umpires_call

### players (6 cols)
match_id, match_type, year, player_name, cricsheet_id, team

## Silver Transformer Design
Script: ingestion/silver_transformer.py

### Modes
- Full:        python ingestion/silver_transformer.py --mode full
               Clears data/silver/, processes all 17k+ files, writes fresh Parquet
- Incremental: python ingestion/silver_transformer.py --mode incremental --since-days 2
               Processes only files with mtime within last N days (default 2)
               Appends new Parquet files to existing partitions (safe — match_ids are unique)

### Implementation
- Parses JSON in Python, accumulates rows in lists, writes Parquet every 1000 files
- Uses pyarrow.parquet.write_to_dataset — avoids holding all rows in memory at once
  (DuckDB executemany and pandas DataFrame both OOM'd on 9.7M delivery rows)
- _s() / _i() helpers in parse_match coerce all fields to consistent Python types
  before pyarrow sees them — guards against older Cricsheet files storing season
  and other fields as integers instead of strings

### Parquet File Layout
data/silver/
  matches/match_type=t20/year=2024/part-uuid.parquet
  innings/match_type=t20/year=2024/part-uuid.parquet
  deliveries/match_type=t20/year=2024/part-uuid.parquet
  players/match_type=t20/year=2024/part-uuid.parquet

Multiple part files per partition is normal — each 1000-file batch writes one file.

## Cricsheet JSON Format — Full Reference
Source: https://cricsheet.org/format/json/

### Meta Section
- data_version: format version string (e.g. "1.1.0")
- created: file creation date YYYY-MM-DD (does not change on revision)
- revision: integer starting at 1, increments on every Cricsheet update; filename never changes

### Info Section
**Match identification**
- match_type: T20 / ODI / Test / IT20 / ODM / MDM (mixed case — normalise to lowercase)
- match_type_number: ordinal sequence number for this match type globally
- gender: "male" or "female"
- team_type: "international" or "club"
- teams: array of exactly two team names
- dates: array of YYYY-MM-DD; always an array; use dates[0] for year partitioning
- season: string e.g. "2018" or "2011/12"
- venue, city: ground name and city (both optional)
- balls_per_over: integer, usually 6
- overs: max overs per innings (20 for T20, 50 for ODI; absent for Test)

**Event**
- event.name: tournament / series name
- event.match_number: match sequence within event
- event.group: group stage label (optional)
- event.stage: e.g. "Final", "Super 10" (optional)

**Toss**
- toss.winner: team name
- toss.decision: "bat" or "field"
- toss.uncontested: boolean, true if toss was automatically awarded (optional)

**Outcome**
- outcome.winner: winning team name (absent if no winner)
- outcome.result: "draw" / "no result" / "tie" (present when no winner)
- outcome.method: "D/L" / "VJD" / "Awarded" / "1st innings score" / "Lost fewer wickets" (optional)
- outcome.eliminator: super-over winner (optional)
- outcome.bowl_out: bowl-out winner (optional)
- outcome.by.runs: margin in runs (optional)
- outcome.by.wickets: margin in wickets (optional)
- outcome.by.innings: innings margin, value is always 1 (optional)

**Players & registry**
- players: object mapping team name → array of player names (playing XI)
- registry.people: object mapping every person mentioned → 8-char Cricsheet UUID
  includes umpires and officials — use info.players for playing XI only
- supersubs: object mapping team name → supersub player name (optional)
- player_of_match: array of player names (optional)
- missing: array listing data absent from this file (optional)

**Officials**
- officials.umpires, officials.tv_umpires, officials.reserve_umpires, officials.match_referees
  each is an array of name strings (all optional)

**Bowl-out (rare)**
- bowl_out: array of objects with bowler (string) and outcome ("hit" or "miss")

### Innings Section
- team: batting team name
- overs: array of over objects
- declared: boolean, true if batting team declared (optional)
- forfeited: boolean, true if innings was forfeited (optional)
- super_over: boolean, true if this is a super over innings (optional)
- absent_hurt: array of player names unable to bat due to injury (optional)
- target.runs: target score (second innings only)
- target.overs: target overs for D/L (optional)
- penalty_runs.pre: runs awarded before this innings (optional)
- penalty_runs.post: runs awarded after this innings (optional)
- powerplays: array of {from, to, type} where type is "batting"/"fielding"/"mandatory"
- miscounted_overs: object mapping over number → {balls, umpire} for umpire error overs (rare)

### Delivery Data
Each entry in overs[].deliveries[]:

**Core (all required)**
- batter, bowler, non_striker: player name strings
- runs.batter: runs credited to batter
- runs.extras: extra runs on this delivery
- runs.total: total runs (batter + extras)
- runs.non_boundary: boolean, true when 4 or 6 runs were scored but NOT from a boundary
  (e.g. overthrows, all-run 4) — critical for boundary analysis

**Extras (all optional, default 0)**
- extras.wides, extras.noballs, extras.byes, extras.legbyes, extras.penalty

**Wickets (optional array — usually 0 or 1 elements, rarely 2)**
Each wicket object:
- kind: bowled / caught / caught and bowled / lbw / stumped / run out /
        hit wicket / obstructing the field / retired hurt / retired out /
        hit the ball twice / handled the ball / timed out
- player_out: dismissed player name
- fielders: array of {name} objects

**Review (optional)**
- review.by: team name requesting review
- review.batter: player under review
- review.decision: "struck down" or "upheld"
- review.umpire: umpire whose call is being reviewed (optional)
- review.umpires_call: boolean, true if struck down via umpire's call

**Replacements (optional, rare)**
- replacements.match[]: full player substitutions — {in, out, reason, team}
  reasons: concussion_substitute / covid_replacement / injury_substitute /
           national_callup / national_release / supersub / tactical_substitute / unknown
- replacements.role[]: fielding/bowling role changes — {in, out, reason, role}
  role: "batter" or "bowler"

### Important Pipeline Notes
- match_type is mixed case in JSON — always normalise to lowercase
- dates is always an array — use dates[0] for year partitioning
- registry.people includes officials — use info.players for playing XI
- revision must be checked on every incremental load; filename never changes
- wickets is an array — multiple dismissals per delivery are possible (rare)
- extras fields are optional — default to 0 if absent
- runs.non_boundary is optional — treat NULL as false (boundary was real)
- review and replacements are rare but must be handled without erroring
- season and some fields are stored as integers in older files (e.g. 2018 not "2018")
  — always coerce with _s()/_i() helpers before writing to Parquet
- Optional columns (outcome_eliminator, review_by, super_over, etc.) may be inferred
  as NULL type in Parquet batches where they never appear — always use union_by_name=true
  in read_parquet() calls to reconcile schemas across batch files