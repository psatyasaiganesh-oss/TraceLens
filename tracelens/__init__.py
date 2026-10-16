"""TraceLens: dependency-free distributed trace collection and analysis."""

from .errors import ConflictError, NotFoundError, StorageError, TraceLensError, ValidationError
from .model import Span, TraceContext
from .store import TraceStore

__all__ = [
    "ConflictError",
    "NotFoundError",
    "Span",
    "StorageError",
    "TraceContext",
    "TraceLensError",
    "TraceStore",
    "ValidationError",
]
__version__ = "1.0.0"
