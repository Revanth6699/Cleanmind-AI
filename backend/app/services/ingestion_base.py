# app/services/ingestion_base.py

from typing import Callable, Dict
import pandas as pd

# Type alias for all reader functions
ReaderFn = Callable[[str], pd.DataFrame]

# Global registry: extension -> reader function
_reader_registry: Dict[str, ReaderFn] = {}


def register_reader(extensions):
    """
    Decorator to register a file reader function for one or more extensions.

    Example:
        @register_reader(["csv"])
        def read_csv(path: str) -> pd.DataFrame:
            ...
    """
    if isinstance(extensions, str):
        extensions = [extensions]

    def decorator(func: ReaderFn) -> ReaderFn:
        for ext in extensions:
            ext_norm = ext.lower().lstrip(".")
            _reader_registry[ext_norm] = func
        return func

    return decorator


def get_reader(ext: str) -> ReaderFn:
    """
    Return the registered reader for a given file extension.
    Raises ValueError if no reader is registered.
    """
    ext_norm = ext.lower().lstrip(".")
    try:
        return _reader_registry[ext_norm]
    except KeyError:
        raise ValueError(f"No reader registered for extension: {ext_norm}")
