"""Expected TraceLens failures."""


class TraceLensError(Exception):
    """Base class for safe, user-facing errors."""


class ValidationError(TraceLensError):
    """Input or a trace graph failed validation."""


class ConflictError(TraceLensError):
    """An identifier was reused for different immutable data."""


class NotFoundError(TraceLensError):
    """A requested trace was not found."""


class StorageError(TraceLensError):
    """A collector capacity or state constraint was violated."""
