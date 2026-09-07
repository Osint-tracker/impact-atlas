"""Typed, validated runtime configuration and project paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping

from dotenv import load_dotenv

from impact_atlas.errors import ConfigurationError


def _positive_int(value: str | None, default: int, name: str) -> int:
    """Parse a positive environment integer or return its documented default."""
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer, got {value!r}.") from error
    if parsed <= 0:
        raise ConfigurationError(f"{name} must be greater than zero, got {parsed}.")
    return parsed


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """Resolved locations used by the application at runtime."""

    root: Path
    assets_data: Path
    data: Path
    logs: Path
    output: Path
    impact_database: Path
    raw_events_database: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> ProjectPaths:
        """Build project paths from an explicit root or this package's parent."""
        resolved_root = (root or Path(__file__).resolve().parents[1]).resolve()
        return cls(
            root=resolved_root,
            assets_data=resolved_root / "assets" / "data",
            data=resolved_root / "data",
            logs=resolved_root / "logs",
            output=resolved_root / "reports",
            impact_database=resolved_root / "impact_atlas.db",
            raw_events_database=resolved_root / "war_tracker_v2" / "data" / "raw_events.db",
        )

    def ensure_runtime_directories(self) -> None:
        """Create only the directories that the application is allowed to own."""
        for directory in (self.assets_data, self.data, self.logs, self.output, self.raw_events_database.parent):
            directory.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Validated runtime controls sourced from environment variables."""

    request_timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    user_agent: str
    firms_api_key: str | None
    openrouter_api_key: str | None

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        dotenv_path: Path | None = None,
    ) -> RuntimeSettings:
        """Load and validate environment-backed settings without logging secrets."""
        if dotenv_path is not None:
            load_dotenv(dotenv_path, override=False)
        else:
            load_dotenv(override=False)
        values = environment or os.environ
        try:
            retry_backoff = float(values.get("IMPACT_ATLAS_RETRY_BACKOFF_SECONDS", "1.5"))
        except ValueError as error:
            raise ConfigurationError("IMPACT_ATLAS_RETRY_BACKOFF_SECONDS must be numeric.") from error
        if retry_backoff <= 0:
            raise ConfigurationError("IMPACT_ATLAS_RETRY_BACKOFF_SECONDS must be greater than zero.")
        return cls(
            request_timeout_seconds=_positive_int(
                values.get("IMPACT_ATLAS_REQUEST_TIMEOUT_SECONDS"), 30, "IMPACT_ATLAS_REQUEST_TIMEOUT_SECONDS"
            ),
            max_retries=_positive_int(values.get("IMPACT_ATLAS_MAX_RETRIES"), 3, "IMPACT_ATLAS_MAX_RETRIES"),
            retry_backoff_seconds=retry_backoff,
            user_agent=values.get(
                "IMPACT_ATLAS_USER_AGENT",
                "ImpactAtlas/2.0 (+https://github.com/Osint-tracker/impact-atlas)",
            ),
            firms_api_key=values.get("FIRMS_API_KEY") or None,
            openrouter_api_key=values.get("OPENROUTER_API_KEY") or None,
        )

    def require(self, setting_name: str, value: str | None) -> str:
        """Return a required secret value or raise a clear configuration error."""
        if not value:
            raise ConfigurationError(f"{setting_name} must be configured before this operation can run.")
        return value
