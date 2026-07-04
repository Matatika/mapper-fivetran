"""Shared, dependency-light helpers used by both the record and Arrow BATCH paths."""

from __future__ import annotations

import functools
import sys
import uuid

import humps

if sys.version_info >= (3, 14):
    # UUIDv7 is monotonic (time-ordered), which keeps BATCH output filenames
    # sortable by creation order; fall back to UUIDv4 on older interpreters
    # where uuid.uuid7() doesn't exist.
    def new_uuid() -> uuid.UUID:
        """Generate a UUIDv7."""
        return uuid.uuid7()
else:

    def new_uuid() -> uuid.UUID:
        """Generate a UUIDv4."""
        return uuid.uuid4()


@functools.cache
def transform_name(name: str) -> str:
    """Convert a property name of any casing convention to snake_case.

    Shared by the record-based `FivetranStreamMap` transform and the vectorized
    Arrow BATCH column renaming, so both paths normalize names identically.

    Column names repeat many times over (once per record, or once per column
    per batch), and the humps round-trip below is comparatively expensive, so
    memoize on the (small, bounded) set of distinct names.

    Args:
        name: The raw property name.

    Returns:
        The snake_case equivalent.
    """
    # handle names with mixed casing, underscores and capital subsequences
    transformed_parts = [
        part.lower() if part.isupper() else humps.decamelize(humps.camelize(part))
        for part in name.split("_")
    ]

    transformed = "_".join(transformed_parts)
    return transformed.replace(".", "_")
