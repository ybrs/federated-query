"""Generate deterministic, related game-studio fixtures without source access."""

import argparse
from datetime import datetime, timedelta
import random

import pyarrow as pa
import pyarrow.parquet as pq

from schema import OUTPUT, SCHEMAS, fixture_path

START = datetime(2026, 1, 1)
CHANNELS = ("organic", "search", "creator", "social")
PLANS = ("free", "monthly", "annual")


def append_row(tables, name, *values):
    """Assign a stable primary key and append one explicitly named record."""
    row = dict(zip(SCHEMAS[name].names, (len(tables[name]) + 1, *values), strict=True))
    tables[name].append(row)


def web_journey(tables, visitor_id, player_id, channel, registered):
    """Record anonymous page views, then a linked registration when it occurs."""
    viewed = registered - timedelta(hours=2)
    append_row(tables, "web_events", visitor_id, None, viewed, "page_view", channel)
    if player_id is not None:
        registration(tables, visitor_id, player_id, channel, registered)


def registration(tables, visitor_id, player_id, channel, registered):
    """Connect a signup to the visitor identity after the anonymous page view."""
    started = registered - timedelta(hours=1)
    append_row(
        tables, "web_events", visitor_id, None, started, "signup_started", channel
    )
    identity = (visitor_id, player_id, registered, "registration", channel)
    append_row(tables, "web_events", *identity)


def payment(tables, player_id, paid_at, plan, rng):
    """Record settled subscriptions and both settled and failed item payments."""
    prices = {"monthly": 999, "annual": 7999}
    if plan != "free":
        values = (player_id, paid_at, "subscription", "settled", prices[plan])
        append_row(tables, "transactions", *values)
    for _ in range(rng.randrange(4)):
        status = rng.choice(("settled", "settled", "failed"))
        purchased = paid_at + timedelta(days=3)
        values = (player_id, purchased, "item", status, rng.choice((299, 599, 1299)))
        append_row(tables, "transactions", *values)


def ticket(tables, player_id, opened, category, rng):
    """Record support effort and retain genuinely unresolved tickets as NULL."""
    closed = None
    if rng.random() < 0.8:
        closed = opened + timedelta(hours=rng.randrange(1, 24))
    append_row(
        tables, "tickets", player_id, opened, closed, category, rng.randrange(5, 46)
    )


def gameplay(tables, player_id, registered, rng):
    """Generate sessions and a version-associated level-7 failure pattern."""
    for day in range(1, 22):
        if rng.random() < 0.35:
            continue
        occurred = registered + timedelta(days=day, hours=1)
        version = "1.1" if occurred.day >= 15 else "1.0"
        level = rng.randrange(1, 11)
        append_row(
            tables, "events", player_id, occurred, "session_start", level, version
        )
        level_outcome(tables, player_id, occurred, level, version, rng)


def level_outcome(tables, player_id, occurred, level, version, rng):
    """Link some failed attempts to later gameplay support contacts."""
    failure_rate = 0.75 if (level, version) == (7, "1.1") else 0.12
    outcome = "level_failed" if rng.random() < failure_rate else "level_up"
    append_row(
        tables,
        "events",
        player_id,
        occurred + timedelta(minutes=5),
        outcome,
        level,
        version,
    )
    gameplay_contact(tables, player_id, occurred, outcome, rng)


def gameplay_contact(tables, player_id, occurred, outcome, rng):
    """Create a later contact for a subset of failed attempts."""
    if outcome == "level_failed" and rng.random() < 0.55:
        ticket(tables, player_id, occurred + timedelta(hours=2), "gameplay", rng)


def populate(players, seed):
    """Build related facts with independent plan and channel assignments."""
    rng = random.Random(seed)
    tables = {}
    for name in SCHEMAS:
        tables[name] = []
    for player_id in range(1, players + 1):
        registered = START + timedelta(days=rng.randrange(7), hours=12)
        plan = rng.choices(PLANS, weights=(6, 3, 1))[0]
        append_row(tables, "players", registered, plan)
        web_journey(tables, player_id, player_id, rng.choice(CHANNELS), registered)
        payment(tables, player_id, registered + timedelta(hours=1), plan, rng)
        gameplay(tables, player_id, registered, rng)
        if rng.random() < 0.25:
            ticket(tables, player_id, registered + timedelta(days=4), "billing", rng)
    return tables


def main():
    """Write typed fixture files and anonymous journeys for the requested cohort."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--players", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.players < 30:
        parser.error("--players must be at least 30")
    tables = populate(args.players, args.seed)
    for visitor_id in range(args.players + 1, 2 * args.players + 1):
        registered = START + timedelta(hours=12)
        web_journey(tables, visitor_id, None, CHANNELS[visitor_id % 4], registered)
    OUTPUT.mkdir(exist_ok=True)
    (OUTPUT / "acquisition").mkdir(exist_ok=True)
    for name, rows in tables.items():
        table = pa.Table.from_pylist(rows, schema=SCHEMAS[name])
        pq.write_table(table, fixture_path(name))
        print(f"{name}: {table.num_rows} rows")


if __name__ == "__main__":
    main()
