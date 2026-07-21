"""XNYS session materialization for the advisory review scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as exchange_calendars
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from app.persistence.tables import trading_sessions


_ET = ZoneInfo("America/New_York")
_CALENDAR = exchange_calendars.get_calendar("XNYS")
_NON_LIVE_STATUSES = frozenset({"REPLAY", "SHADOW"})


@dataclass(frozen=True)
class MarketSessionSchedule:
    trading_date: date
    opened_at_utc: datetime
    closed_at_utc: datetime


def xnys_session_schedule(trading_day: date) -> MarketSessionSchedule | None:
    label = trading_day.isoformat()
    if not bool(_CALENDAR.is_session(label)):
        return None
    opened = _CALENDAR.session_open(label).to_pydatetime()
    closed = _CALENDAR.session_close(label).to_pydatetime()
    return MarketSessionSchedule(
        trading_date=trading_day,
        opened_at_utc=_aware_utc(opened),
        closed_at_utc=_aware_utc(closed),
    )


def materialize_recent_xnys_sessions(
    engine: Engine,
    *,
    now: datetime,
    lookback_days: int = 7,
) -> list[MarketSessionSchedule]:
    """Create/close recent exchange sessions without inventing holiday sessions."""
    now = _aware_utc(now)
    if not 0 <= lookback_days <= 30:
        raise ValueError("XNYS materialization lookback is invalid")
    current_et_date = now.astimezone(_ET).date()
    schedules: list[MarketSessionSchedule] = []
    for offset in range(lookback_days, -1, -1):
        trading_day = current_et_date - timedelta(days=offset)
        schedule = xnys_session_schedule(trading_day)
        if schedule is None:
            continue
        _materialize_schedule(engine, schedule, now)
        schedules.append(schedule)
    return schedules


def _materialize_schedule(
    engine: Engine,
    schedule: MarketSessionSchedule,
    now: datetime,
) -> None:
    with engine.begin() as conn:
        row = (
            conn.execute(
                select(trading_sessions)
                .where(trading_sessions.c.trading_date == schedule.trading_date)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        closed = now >= schedule.closed_at_utc
        if row is None:
            values = {
                "session_id": f"xnys_{schedule.trading_date.isoformat()}",
                "trading_date": schedule.trading_date,
                "status": "CLOSED" if closed else "OPEN",
                "opened_at_utc": schedule.opened_at_utc,
                "closed_at_utc": schedule.closed_at_utc if closed else None,
                "created_at_utc": now,
            }
            if conn.dialect.name == "postgresql":
                conn.execute(
                    postgresql_insert(trading_sessions)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=[trading_sessions.c.trading_date])
                )
            elif conn.dialect.name == "sqlite":
                conn.execute(
                    sqlite_insert(trading_sessions)
                    .values(**values)
                    .on_conflict_do_nothing(index_elements=[trading_sessions.c.trading_date])
                )
            else:
                conn.execute(trading_sessions.insert().values(**values))
            return

        existing_open = _optional_aware_utc(row["opened_at_utc"])
        existing_close = _optional_aware_utc(row["closed_at_utc"])
        if existing_open is not None and existing_open != schedule.opened_at_utc:
            raise ValueError("persisted trading-session open conflicts with XNYS calendar")
        if existing_close is not None and existing_close != schedule.closed_at_utc:
            raise ValueError("persisted trading-session close conflicts with XNYS calendar")
        status = str(row["status"])
        if status in _NON_LIVE_STATUSES:
            return
        if status == "CLOSED" and not closed:
            raise ValueError("trading session was closed before the XNYS close")
        conn.execute(
            update(trading_sessions)
            .where(trading_sessions.c.session_id == row["session_id"])
            .values(
                status="CLOSED" if closed else status,
                opened_at_utc=existing_open or schedule.opened_at_utc,
                closed_at_utc=schedule.closed_at_utc if closed else None,
            )
        )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("XNYS calendar returned a naive timestamp")
    return value.astimezone(UTC)


def _optional_aware_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError("persisted trading-session timestamp is invalid")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


__all__ = [
    "MarketSessionSchedule",
    "materialize_recent_xnys_sessions",
    "xnys_session_schedule",
]
