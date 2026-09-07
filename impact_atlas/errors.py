"""Application-specific exception types used at operational boundaries."""


class ImpactAtlasError(Exception):
    """Base class for expected, actionable application failures."""


class ConfigurationError(ImpactAtlasError):
    """Raised when required runtime configuration is absent or invalid."""


class DataValidationError(ImpactAtlasError):
    """Raised when an external record cannot satisfy a required schema."""


class PersistenceError(ImpactAtlasError):
    """Raised when a transactional persistence operation cannot complete."""


class IngestionError(ImpactAtlasError):
    """Raised when a source ingestion step fails beyond per-record tolerance."""


class ExportError(ImpactAtlasError):
    """Raised when a required output artifact cannot be produced."""


class ProviderError(ImpactAtlasError):
    """Raised when an external AI/data provider rejects or fails a request."""
