"""
IPL player stats validation script.
Queries gold.duckdb and prints batting + bowling career figures for 10 players.
Cross-check the output against ESPNcricinfo IPL stats pages.

Uses int_batting_by_innings / int_bowling_by_innings which carry the
super_over flag — both are excluded from all career aggregations here.
"""

import duckdb
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)

con = duckdb.connect(r"data\gold.duckdb", read_only=True)

# ---------------------------------------------------------------------------
# Players to validate
# ---------------------------------------------------------------------------
PLAYERS = [
    "V Kohli",
    "RG Sharma",
    "MS Dhoni",
    "SK Raina",
    "DA Warner",
    "AB de Villiers",
    "CH Gayle",
    "KL Rahul",
    "SL Malinga",
    "JJ Bumrah",
]

IN_LIST = ", ".join(f"'{p}'" for p in PLAYERS)

# ---------------------------------------------------------------------------
# Known reference stats (ESPNcricinfo, all IPL editions up to and including 2025)
# These are approximate — use them as a sanity-check range, not exact targets.
# Source: https://www.espncricinfo.com/records/tournament/batting-most-runs-career/indian-premier-league-117
# ---------------------------------------------------------------------------
BAT_REF = {
    # player: (runs_ref, hs_ref, centuries_ref, fifties_ref, note)
    # Retired players — career closed, exact figures from ESPNcricinfo.
    "DA Warner":       (6565, 126, 4, 62,  "retired — final career figures"),
    "SK Raina":        (5528, 100, 1, 39,  "retired — final career figures"),
    "AB de Villiers":  (5162, 133, 3, 40,  "retired — final career figures"),
    "CH Gayle":        (4965, 175, 6, 31,  "retired — final career figures"),
    # Active players — IPL 2026 is ongoing; DB may lag 1–2 matches behind live count.
    "V Kohli":         (8908, 113, 8, 65,  "active — DB snapshot, verify live"),
    "RG Sharma":       (7183, 109, 2, 48,  "active — DB snapshot, verify live"),
    "MS Dhoni":        (5439,  84, 0, 24,  "active — confirmed exact"),
    "KL Rahul":        (5427, 132, 5, 42,  "active — DB snapshot (6th 100 today 25-Apr-26, not yet ingested)"),
    "SL Malinga":      (None, None, None, None, "retired — bat only reference N/A"),
    "JJ Bumrah":       (None, None, None, None, "retired bat reference N/A"),
}

BOWL_REF = {
    # player: (wickets_ref, note)
    # Retired bowlers — career closed, exact figures from ESPNcricinfo.
    "SL Malinga":      (170, "retired — final career figures"),
    # Active bowlers — IPL 2026 ongoing; Bumrah has 0 wkts in first 5 IPL-2026 matches.
    "JJ Bumrah":       (183, "active — 183 pre-2026 + 0 in early IPL-2026; DB may differ by recency"),
}

# ---------------------------------------------------------------------------
# Batting career query
# Uses int_batting_by_innings (has super_over, dismissals, not_out pre-computed)
# Joins fct_match_results for event_name filter
# ---------------------------------------------------------------------------
bat_sql = f"""
SELECT
    b.player_name                                                       AS player,
    count(DISTINCT b.match_id)                                          AS mat,
    count(*)                                                            AS inns,
    sum(b.not_out::integer)                                             AS no,
    sum(b.runs)                                                         AS runs,
    max(b.runs)                                                         AS hs,
    round(sum(b.runs)::double / nullif(sum(b.dismissals), 0), 2)        AS avg,
    sum(b.balls_faced)                                                  AS bf,
    round(sum(b.runs) * 100.0 / nullif(sum(b.balls_faced), 0), 2)       AS sr,
    sum(CASE WHEN b.runs >= 100 THEN 1 ELSE 0 END)                      AS "100",
    sum(CASE WHEN b.runs >= 50 AND b.runs < 100 THEN 1 ELSE 0 END)      AS "50",
    sum(CASE WHEN b.runs = 0 AND NOT b.not_out THEN 1 ELSE 0 END)       AS ducks,
    sum(b.fours)                                                        AS "4s",
    sum(b.sixes)                                                        AS "6s"
FROM main_intermediate.int_batting_by_innings b
JOIN main_marts.fct_match_results m ON b.match_id = m.match_id
WHERE b.player_name IN ({IN_LIST})
  AND m.event_name ILIKE '%indian premier league%'
  AND b.innings_number IN (1, 2)
  AND NOT b.super_over
GROUP BY b.player_name
ORDER BY runs DESC
"""

# ---------------------------------------------------------------------------
# Bowling career query
# Uses int_bowling_by_innings (has super_over, wickets_credited pre-computed)
# ---------------------------------------------------------------------------
bowl_sql = f"""
SELECT
    b.player_name                                                                       AS player,
    count(DISTINCT b.match_id)                                                          AS mat,
    count(*)                                                                            AS inns,
    (floor(sum(b.legal_deliveries) / 6)::integer
     || '.' || (sum(b.legal_deliveries) % 6))::varchar                                 AS overs,
    sum(b.legal_deliveries)                                                             AS balls,
    sum(b.runs_conceded)                                                                AS runs,
    sum(b.wickets_credited)                                                             AS wkts,
    round(sum(b.runs_conceded)::double / nullif(sum(b.wickets_credited), 0), 2)         AS avg,
    round(sum(b.runs_conceded) * 6.0 / nullif(sum(b.legal_deliveries), 0), 2)          AS econ,
    round(sum(b.legal_deliveries)::double / nullif(sum(b.wickets_credited), 0), 2)     AS sr,
    sum(CASE WHEN b.wickets_credited >= 4 THEN 1 ELSE 0 END)                           AS "4w",
    sum(CASE WHEN b.wickets_credited >= 5 THEN 1 ELSE 0 END)                           AS "5w"
FROM main_intermediate.int_bowling_by_innings b
JOIN main_marts.fct_match_results m ON b.match_id = m.match_id
WHERE b.player_name IN ({IN_LIST})
  AND m.event_name ILIKE '%indian premier league%'
  AND b.innings_number IN (1, 2)
  AND NOT b.super_over
GROUP BY b.player_name
ORDER BY wkts DESC
"""

# ---------------------------------------------------------------------------
# Run queries
# ---------------------------------------------------------------------------
bat_df  = con.sql(bat_sql).df()
bowl_df = con.sql(bowl_sql).df()

# ---------------------------------------------------------------------------
# Print batting results + reference comparison
# ---------------------------------------------------------------------------
print("\n" + "=" * 110)
print("IPL CAREER BATTING — computed from gold.duckdb  (super overs excluded)")
print("=" * 110)
print(bat_df.to_string(index=False))

print("\n--- Batting reference check (ESPNcricinfo, exact) ---")
print(f"{'Player':<20} {'DB_runs':>8} {'Ref_runs':>9} {'DB_100':>7} {'Ref_100':>8} "
      f"{'DB_50':>6} {'Ref_50':>7}  {'Status':<8}  Note")
print("-" * 110)
for _, row in bat_df.iterrows():
    ref = BAT_REF.get(row["player"])
    if ref is None:
        continue
    r_runs, r_hs, r_100, r_50, note = ref
    if r_runs is None:
        continue
    runs_ok  = int(row["runs"])  == r_runs
    cent_ok  = int(row["100"])   == r_100
    fifty_ok = int(row["50"])    == r_50
    flag = "OK" if all([runs_ok, cent_ok, fifty_ok]) else "MISMATCH"
    runs_diff = int(row["runs"]) - r_runs
    print(f"{row['player']:<20} {int(row['runs']):>8} {r_runs:>9} {int(row['100']):>7} {r_100:>8} "
          f"{int(row['50']):>6} {r_50:>7}  {flag:<8}  {note}"
          + (f"  [runs diff: {runs_diff:+d}]" if not runs_ok else ""))

# ---------------------------------------------------------------------------
# Print bowling results + reference comparison
# ---------------------------------------------------------------------------
print("\n" + "=" * 110)
print("IPL CAREER BOWLING — computed from gold.duckdb  (super overs excluded)")
print("=" * 110)
print(bowl_df.to_string(index=False))

print("\n--- Bowling reference check (ESPNcricinfo, exact) ---")
print(f"{'Player':<20} {'DB_wkts':>8} {'Ref_wkts':>9}  {'Status':<8}  Note")
print("-" * 80)
for _, row in bowl_df.iterrows():
    ref = BOWL_REF.get(row["player"])
    if ref is None:
        continue
    r_wkts, note = ref
    wkts_ok = int(row["wkts"]) == r_wkts
    flag = "OK" if wkts_ok else "MISMATCH"
    diff = int(row["wkts"]) - r_wkts
    print(f"{row['player']:<20} {int(row['wkts']):>8} {r_wkts:>9}  {flag:<8}  {note}"
          + (f"  [diff: {diff:+d}]" if not wkts_ok else ""))

print()
