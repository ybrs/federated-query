"""Explicit fixture schemas shared by generation, loading, and verification."""

from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "generated"


def fixture_path(name):
    """Keep acquisition files separate from the database-loading fixtures."""
    directory = OUTPUT / "acquisition" if name == "web_events" else OUTPUT
    return directory / f"{name}.parquet"


ROOT_TABLES = {
    "players": "billing.public.players",
    "transactions": "billing.public.transactions",
    "tickets": "crm.support.tickets",
    "events": "gameplay.gameplay.events",
    "web_events": "acquisition.main.web_events",
}

SCHEMAS = {
    "players": pa.schema(
        [
            ("player_id", pa.int64()),
            ("registered_at", pa.timestamp("us")),
            ("subscription_plan", pa.string()),
        ]
    ),
    "transactions": pa.schema(
        [
            ("transaction_id", pa.int64()),
            ("player_id", pa.int64()),
            ("paid_at", pa.timestamp("us")),
            ("kind", pa.string()),
            ("status", pa.string()),
            ("amount_cents", pa.int64()),
        ]
    ),
    "tickets": pa.schema(
        [
            ("ticket_id", pa.int64()),
            ("player_id", pa.int64()),
            ("opened_at", pa.timestamp("us")),
            ("closed_at", pa.timestamp("us")),
            ("category", pa.string()),
            ("handling_minutes", pa.int64()),
        ]
    ),
    "events": pa.schema(
        [
            ("event_id", pa.int64()),
            ("player_id", pa.int64()),
            ("occurred_at", pa.timestamp("us")),
            ("event_name", pa.string()),
            ("level_number", pa.int64()),
            ("game_version", pa.string()),
        ]
    ),
    "web_events": pa.schema(
        [
            ("web_event_id", pa.int64()),
            ("visitor_id", pa.int64()),
            ("player_id", pa.int64()),
            ("occurred_at", pa.timestamp("us")),
            ("event_name", pa.string()),
            ("channel", pa.string()),
        ]
    ),
}
