"""Resilient HTTP transport with explicit retries, timeouts, and rate limits."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable, Mapping

import requests


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Controls bounded retry behavior for transient HTTP failures."""

    max_attempts: int = 3
    timeout_seconds: int = 30
    backoff_seconds: float = 1.5


class ResilientHttpClient:
    """A reusable requests transport with safe defaults and observable failures."""

    _TRANSIENT_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __init__(
        self,
        *,
        name: str,
        user_agent: str,
        retry_policy: RetryPolicy = RetryPolicy(),
        rate_limit_seconds: float = 0.0,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a transport. Injectable time/session dependencies keep it unit-testable."""
        if retry_policy.max_attempts <= 0 or retry_policy.timeout_seconds <= 0 or retry_policy.backoff_seconds <= 0:
            raise ValueError("RetryPolicy values must be positive.")
        if rate_limit_seconds < 0:
            raise ValueError("rate_limit_seconds cannot be negative.")
        self._logger = logging.getLogger(f"impact_atlas.http.{name}")
        self._policy = retry_policy
        self._rate_limit_seconds = rate_limit_seconds
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent})
        self._sleeper = sleeper
        self._clock = clock
        self._last_request_at = 0.0

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response | None:
        """Return a successful response or ``None`` after a logged, bounded failure."""
        request_headers = dict(headers or {})
        for attempt in range(1, self._policy.max_attempts + 1):
            self._respect_rate_limit()
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    headers=request_headers or None,
                    timeout=self._policy.timeout_seconds,
                    **kwargs,
                )
            except requests.RequestException as error:
                if attempt == self._policy.max_attempts:
                    self._logger.error("HTTP request exhausted retries: %s %s (%s)", method, url, error)
                    return None
                self._logger.warning("HTTP transport error; retrying %s %s (%s)", method, url, error)
                self._sleeper(self._policy.backoff_seconds * attempt)
                continue

            if 200 <= response.status_code < 300:
                return response
            if response.status_code not in self._TRANSIENT_STATUS_CODES:
                self._logger.warning("HTTP request rejected: %s %s returned %s", method, url, response.status_code)
                return None
            if attempt == self._policy.max_attempts:
                self._logger.error(
                    "HTTP request exhausted retries: %s %s returned %s",
                    method, url, response.status_code,
                )
                return None
            self._logger.warning(
                "Transient HTTP response; retrying %s %s after status %s", method, url, response.status_code
            )
            self._sleeper(self._policy.backoff_seconds * attempt)
        return None

    def close(self) -> None:
        """Release pooled sockets held by the underlying requests session."""
        self._session.close()

    def _respect_rate_limit(self) -> None:
        """Wait just long enough to satisfy the configured minimum request spacing."""
        if self._rate_limit_seconds <= 0:
            return
        elapsed = self._clock() - self._last_request_at
        if elapsed < self._rate_limit_seconds:
            self._sleeper(self._rate_limit_seconds - elapsed)
        self._last_request_at = self._clock()
