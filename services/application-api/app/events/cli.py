"""Validate/build one EventContext and optionally persist it to PostgreSQL."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from sqlalchemy import create_engine

from app.events import EventContextStore
from app.persistence import persist_event_context


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-dir", type=Path, required=True)
    parser.add_argument("--at-utc", help="RFC3339 UTC instant; defaults to now")
    parser.add_argument("--session-id", default="event-import")
    parser.add_argument("--persist", action="store_true")
    args = parser.parse_args()

    now = (
        datetime.fromisoformat(args.at_utc.replace("Z", "+00:00")).astimezone(timezone.utc)
        if args.at_utc
        else datetime.now(timezone.utc)
    )
    context = EventContextStore(args.event_dir).get(now)
    print(json.dumps(context.model_dump(mode="json"), ensure_ascii=True, indent=2))
    if args.persist:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is required with --persist")
        persist_event_context(create_engine(database_url), args.session_id, context)
    return 0 if context.available else 2


if __name__ == "__main__":
    raise SystemExit(main())
