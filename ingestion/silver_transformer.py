import json
import argparse
import shutil
import time
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

LOCAL_RAW = Path("data/raw")
LOCAL_SILVER = Path("data/silver")

WRITE_BATCH_SIZE = 1000  # files per Parquet write

MATCH_COLS = [
    "match_id", "match_type", "year", "match_date", "season", "venue", "city",
    "gender", "team_type", "team_1", "team_2", "toss_winner", "toss_decision",
    "toss_uncontested", "outcome_winner", "outcome_result", "outcome_method",
    "outcome_eliminator", "outcome_bowl_out", "outcome_by_runs", "outcome_by_wickets",
    "outcome_by_innings", "player_of_match", "event_name", "event_match_number",
    "event_group", "event_stage", "balls_per_over", "overs", "match_type_number",
    "revision",
]

INNINGS_COLS = [
    "match_id", "match_type", "year", "innings_number", "batting_team",
    "declared", "forfeited", "super_over",
    "target_runs", "target_overs", "penalty_runs_pre", "penalty_runs_post",
]

DELIVERY_COLS = [
    "match_id", "match_type", "year", "innings_number", "over_number", "ball_in_over",
    "batter", "bowler", "non_striker",
    "runs_batter", "runs_extras", "runs_total", "runs_non_boundary",
    "extras_wides", "extras_noballs", "extras_byes", "extras_legbyes", "extras_penalty",
    "wicket_count", "is_wicket", "wicket_kind", "wicket_player_out", "wicket_fielders",
    "review_by", "review_decision", "review_umpires_call",
]

PLAYER_COLS = [
    "match_id", "match_type", "year", "player_name", "cricsheet_id", "team",
]


def _s(v):
    """Coerce to str or None — guards against fields stored as int in older files."""
    return str(v) if v is not None else None


def _i(v):
    """Coerce to int or None."""
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def parse_match(path: Path, data: dict):
    match_id = path.stem
    info = data.get("info", {})
    meta = data.get("meta", {})

    match_type = info.get("match_type", "unknown").lower()
    dates = info.get("dates", [])
    year = str(dates[0])[:4] if dates else "unknown"

    teams = info.get("teams", [])
    outcome = info.get("outcome", {})
    outcome_by = outcome.get("by", {})
    event = info.get("event", {})
    toss = info.get("toss", {})

    match_row = (
        match_id, match_type, year,
        str(dates[0]) if dates else None,
        _s(info.get("season")),
        _s(info.get("venue")),
        _s(info.get("city")),
        _s(info.get("gender")),
        _s(info.get("team_type")),
        _s(teams[0]) if len(teams) > 0 else None,
        _s(teams[1]) if len(teams) > 1 else None,
        _s(toss.get("winner")),
        _s(toss.get("decision")),
        toss.get("uncontested"),
        _s(outcome.get("winner")),
        _s(outcome.get("result")),
        _s(outcome.get("method")),
        _s(outcome.get("eliminator")),
        _s(outcome.get("bowl_out")),
        _i(outcome_by.get("runs")),
        _i(outcome_by.get("wickets")),
        _i(outcome_by.get("innings")),
        ", ".join(info.get("player_of_match", [])) or None,
        _s(event.get("name")),
        _i(event.get("match_number")),
        _s(event.get("group")),
        _s(event.get("stage")),
        _i(info.get("balls_per_over")),
        _i(info.get("overs")),
        _i(info.get("match_type_number")),
        _i(meta.get("revision", 1)),
    )

    innings_rows = []
    delivery_rows = []

    for innings_num, innings in enumerate(data.get("innings", []), 1):
        batting_team = innings.get("team")
        target = innings.get("target", {})
        penalty = innings.get("penalty_runs", {})

        innings_rows.append((
            match_id, match_type, year,
            innings_num,
            batting_team,
            innings.get("declared"),
            innings.get("forfeited"),
            innings.get("super_over"),
            target.get("runs"),
            target.get("overs"),
            penalty.get("pre"),
            penalty.get("post"),
        ))

        for over_obj in innings.get("overs", []):
            over_number = over_obj.get("over", 0)
            for ball_idx, delivery in enumerate(over_obj.get("deliveries", []), 1):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras", {})
                wickets = delivery.get("wickets", [])
                review = delivery.get("review", {})

                first_wicket = wickets[0] if wickets else {}
                fielders = ", ".join(
                    f.get("name", "") for f in first_wicket.get("fielders", [])
                    if f.get("name")
                ) or None

                delivery_rows.append((
                    match_id, match_type, year,
                    innings_num,
                    over_number,
                    ball_idx,
                    _s(delivery.get("batter")),
                    _s(delivery.get("bowler")),
                    _s(delivery.get("non_striker")),
                    _i(runs.get("batter", 0)),
                    _i(runs.get("extras", 0)),
                    _i(runs.get("total", 0)),
                    runs.get("non_boundary"),
                    _i(extras.get("wides", 0)),
                    _i(extras.get("noballs", 0)),
                    _i(extras.get("byes", 0)),
                    _i(extras.get("legbyes", 0)),
                    _i(extras.get("penalty", 0)),
                    len(wickets),
                    bool(wickets),
                    _s(first_wicket.get("kind")),
                    _s(first_wicket.get("player_out")),
                    fielders,
                    _s(review.get("by")),
                    _s(review.get("decision")),
                    review.get("umpires_call"),
                ))

    registry = info.get("registry", {}).get("people", {})
    players_by_team = info.get("players", {})
    player_rows = []
    for team_name, team_players in players_by_team.items():
        for player_name in team_players:
            player_rows.append((
                match_id, match_type, year,
                player_name,
                registry.get(player_name),
                team_name,
            ))

    return match_row, innings_rows, delivery_rows, player_rows


def write_batch(mb, ib, db, pb):
    for rows, cols, name in [
        (mb, MATCH_COLS, "matches"),
        (ib, INNINGS_COLS, "innings"),
        (db, DELIVERY_COLS, "deliveries"),
        (pb, PLAYER_COLS, "players"),
    ]:
        if not rows:
            continue
        col_data = {col: [row[i] for row in rows] for i, col in enumerate(cols)}
        table = pa.Table.from_pydict(col_data)
        pq.write_to_dataset(
            table,
            root_path=str(LOCAL_SILVER / name),
            partition_cols=["match_type", "year"],
            existing_data_behavior="overwrite_or_ignore",
        )


def transform(mode: str = "full", since_days: int = 2):
    if mode == "full":
        all_files = sorted(LOCAL_RAW.rglob("*.json"))
        if LOCAL_SILVER.exists():
            shutil.rmtree(LOCAL_SILVER)
        LOCAL_SILVER.mkdir(parents=True)
    else:
        cutoff = time.time() - since_days * 86400
        all_files = sorted(
            f for f in LOCAL_RAW.rglob("*.json")
            if f.stat().st_mtime > cutoff
        )
        LOCAL_SILVER.mkdir(parents=True, exist_ok=True)

    total = len(all_files)
    print(f"Mode: {mode} | Files to process: {total}")

    mb, ib, db, pb = [], [], [], []
    errors = 0

    for i, path in enumerate(all_files, 1):
        try:
            data = json.loads(path.read_bytes())
            match_row, innings_rows, delivery_rows, player_rows = parse_match(path, data)
            mb.append(match_row)
            ib.extend(innings_rows)
            db.extend(delivery_rows)
            pb.extend(player_rows)

            if i % 100 == 0:
                print(f"  Processed {i}/{total} ...")

            if i % WRITE_BATCH_SIZE == 0:
                write_batch(mb, ib, db, pb)
                mb, ib, db, pb = [], [], [], []
                print(f"  Wrote batch {i // WRITE_BATCH_SIZE}")

        except Exception as e:
            print(f"  ERROR {path.name}: {e}")
            errors += 1

    write_batch(mb, ib, db, pb)
    print(f"\nDone. Errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["full", "incremental"], default="full",
        help="full: rebuild all silver; incremental: process files modified in last N days",
    )
    parser.add_argument(
        "--since-days", type=int, default=2,
        help="Incremental only: process files modified in the last N days (default 2)",
    )
    args = parser.parse_args()
    transform(args.mode, args.since_days)
