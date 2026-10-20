"""Small, explicit authorization boundary for the loopback administration API."""

from __future__ import annotations

import hmac
import re
import secrets
from dataclasses import dataclass, field
from typing import Literal

Role = Literal["reader", "writer"]
_TOKEN = re.compile(r"[A-Za-z0-9._~-]{32,256}\Z")


@dataclass(frozen=True)
class AdminAccess:
    """Separate read/write bearer capabilities; secrets never appear in repr.

    This is intended for a trusted local workstation. It is not a hosted user
    directory, TLS terminator, or substitute for individual user audit trails.
    Tokens must remain in memory or trusted server configuration, not URLs.
    """

    writer_token: str = field(default_factory=lambda: secrets.token_urlsafe(32), repr=False)
    reader_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Reject weak, ambiguous or HTTP-header-incompatible configuration."""
        for token in (self.writer_token, self.reader_token):
            if token is not None and not _TOKEN.fullmatch(token):
                raise ValueError("Admin tokens must contain 32..256 URL-safe characters")
        if self.reader_token == self.writer_token:
            raise ValueError("Reader and writer tokens must be distinct")

    def role_for(self, authorization: str | None) -> Role | None:
        """Authenticate a bearer header with constant-time secret comparisons."""
        if not authorization:
            return None
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not _TOKEN.fullmatch(token):
            return None
        writer = hmac.compare_digest(token, self.writer_token)
        reader = hmac.compare_digest(token, self.reader_token or self.writer_token)
        if writer:
            return "writer"
        if self.reader_token is not None and reader:
            return "reader"
        return None
