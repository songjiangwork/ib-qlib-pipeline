from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select

from ..dborm.models import Instrument, InstrumentAlias
from ..dborm.session import create_session_factory_for_path


def _session_for_db(db_path: Path):
    return create_session_factory_for_path(db_path)()


def _instrument_to_dict(item: Instrument) -> dict[str, Any]:
    return {
        "id": item.id,
        "canonical_symbol": item.canonical_symbol,
        "display_symbol": item.display_symbol,
        "asset_type": item.asset_type,
        "country": item.country,
        "exchange": item.exchange,
        "currency": item.currency,
        "name_en": item.name_en,
        "name_zh": item.name_zh,
        "sector": item.sector,
        "industry": item.industry,
        "sub_industry": item.sub_industry,
        "business_summary": item.business_summary,
        "website": item.website,
        "ipo_date": item.ipo_date,
        "delist_date": item.delist_date,
        "listing_status": item.listing_status,
        "source": item.source,
        "source_updated_at": item.source_updated_at,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def get_instrument_by_symbol(db_path: Path, canonical_symbol: str) -> dict[str, Any] | None:
    with _session_for_db(db_path) as session:
        item = session.execute(
            select(Instrument).where(Instrument.canonical_symbol == canonical_symbol)
        ).scalar_one_or_none()
    if item is None:
        return None
    return _instrument_to_dict(item)


def list_instruments(
    db_path: Path,
    *,
    country: str | None = None,
    exchange: str | None = None,
    asset_type: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        stmt = select(Instrument).order_by(Instrument.canonical_symbol)
        if country:
            stmt = stmt.where(Instrument.country == country)
        if exchange:
            stmt = stmt.where(Instrument.exchange == exchange)
        if asset_type:
            stmt = stmt.where(Instrument.asset_type == asset_type)
        if limit is not None:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).scalars().all()
    return [_instrument_to_dict(item) for item in rows]


def list_instrument_aliases(db_path: Path, canonical_symbol: str) -> list[dict[str, Any]]:
    with _session_for_db(db_path) as session:
        item = session.execute(
            select(Instrument).where(Instrument.canonical_symbol == canonical_symbol)
        ).scalar_one_or_none()
        if item is None:
            return []
        aliases = session.execute(
            select(InstrumentAlias)
            .where(InstrumentAlias.instrument_id == item.id)
            .order_by(InstrumentAlias.alias_type, InstrumentAlias.alias_value)
        ).scalars().all()
    return [
        {
            "id": alias.id,
            "instrument_id": alias.instrument_id,
            "alias_type": alias.alias_type,
            "alias_value": alias.alias_value,
            "created_at": alias.created_at,
        }
        for alias in aliases
    ]


def upsert_instrument(db_path: Path, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    canonical_symbol = str(payload["canonical_symbol"]).strip().upper()
    with _session_for_db(db_path) as session:
        item = session.execute(
            select(Instrument).where(Instrument.canonical_symbol == canonical_symbol)
        ).scalar_one_or_none()
        created = item is None
        if item is None:
            item = Instrument(
                canonical_symbol=canonical_symbol,
                display_symbol=str(payload.get("display_symbol") or canonical_symbol).strip(),
                asset_type=str(payload["asset_type"]).strip(),
                country=payload.get("country"),
                exchange=payload.get("exchange"),
                currency=payload.get("currency"),
                name_en=payload.get("name_en"),
                name_zh=payload.get("name_zh"),
                sector=payload.get("sector"),
                industry=payload.get("industry"),
                sub_industry=payload.get("sub_industry"),
                business_summary=payload.get("business_summary"),
                website=payload.get("website"),
                ipo_date=payload.get("ipo_date"),
                delist_date=payload.get("delist_date"),
                listing_status=payload.get("listing_status"),
                source=payload.get("source"),
                source_updated_at=payload.get("source_updated_at"),
                created_at=now,
                updated_at=now,
            )
            session.add(item)
            session.commit()
            session.refresh(item)
            return _instrument_to_dict(item), True

        for field in (
            "display_symbol",
            "asset_type",
            "country",
            "exchange",
            "currency",
            "name_en",
            "name_zh",
            "sector",
            "industry",
            "sub_industry",
            "business_summary",
            "website",
            "ipo_date",
            "delist_date",
            "listing_status",
            "source",
            "source_updated_at",
        ):
            if field in payload and payload.get(field) is not None:
                setattr(item, field, payload.get(field))
        item.updated_at = now
        session.commit()
        session.refresh(item)
        return _instrument_to_dict(item), False


def upsert_instrument_alias(
    db_path: Path,
    *,
    canonical_symbol: str,
    alias_type: str,
    alias_value: str,
) -> tuple[dict[str, Any], bool]:
    canonical_symbol = canonical_symbol.strip().upper()
    alias_type = alias_type.strip()
    alias_value = alias_value.strip()
    if not alias_type or not alias_value:
        raise ValueError("alias_type and alias_value are required")
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with _session_for_db(db_path) as session:
        instrument = session.execute(
            select(Instrument).where(Instrument.canonical_symbol == canonical_symbol)
        ).scalar_one_or_none()
        if instrument is None:
            raise ValueError(f"Instrument not found for canonical_symbol={canonical_symbol}")
        existing = session.execute(
            select(InstrumentAlias).where(
                InstrumentAlias.alias_type == alias_type,
                InstrumentAlias.alias_value == alias_value,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return {
                "id": existing.id,
                "instrument_id": existing.instrument_id,
                "alias_type": existing.alias_type,
                "alias_value": existing.alias_value,
                "created_at": existing.created_at,
            }, False
        item = InstrumentAlias(
            instrument_id=int(instrument.id),
            alias_type=alias_type,
            alias_value=alias_value,
            created_at=now,
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        return {
            "id": item.id,
            "instrument_id": item.instrument_id,
            "alias_type": item.alias_type,
            "alias_value": item.alias_value,
            "created_at": item.created_at,
        }, True


def bulk_upsert_instrument_aliases(
    db_path: Path,
    *,
    canonical_symbol: str,
    aliases: Iterable[tuple[str, str]],
) -> tuple[int, int]:
    inserted = 0
    existing = 0
    for alias_type, alias_value in aliases:
        _, created = upsert_instrument_alias(
            db_path,
            canonical_symbol=canonical_symbol,
            alias_type=alias_type,
            alias_value=alias_value,
        )
        if created:
            inserted += 1
        else:
            existing += 1
    return inserted, existing
