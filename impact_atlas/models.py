"""Typed domain models shared across ingestion and export boundaries.

These value objects give the pipeline a single, validated vocabulary for the
entities that cross module boundaries (source records, persisted events,
units, and presentation styles) without coupling the layers together.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UnitRecord:
    """A military unit as persisted in ``units_registry``."""

    unit_id: str
    official_name: str | None = None
    aliases: str | None = None
    faction: str | None = None
    status: str = "ACTIVE"
    equipment_manifest: str | None = None


@dataclass(frozen=True, slots=True)
class KineticEvent:
    """A geolocated event as persisted in ``kinetic_events``."""

    event_id: str
    source: str
    unit_id: str | None = None
    date: str | None = None
    lat: float | None = None
    lon: float | None = None
    intensity_score: float | None = None
    raw_data: str | None = None
    image_phash: str | None = None


@dataclass(frozen=True, slots=True)
class SourceRef:
    """A normalized citation attached to an exported event."""

    name: str
    url: str


@dataclass(frozen=True, slots=True)
class MarkerStyle:
    """Presentation-only sizing/color derived from T.I.E. vectors."""

    radius: float
    color: str
