"""Vectorized Arrow-table transforms for Singer BATCH messages.

Mirrors the per-record transforms in `mapper_fivetran.mapper.FivetranStreamMap`
(name normalization, `_fivetran_synced`, `_fivetran_deleted`, and stringifying
whatever flattening couldn't fully unpack), but operates on whole
`pyarrow.Table` columns instead of looping over rows, so it stays fast on
large batches.

`_fivetran_id` is intentionally not computed here: doing so faithfully would
require hashing each row's full JSON representation, which cannot be
vectorized. Streams without `key_properties` are rejected instead of silently
producing a slow or incorrect id column; see `assert_batch_supported`.
"""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone

import msgspec
import pyarrow as pa
import pyarrow.compute as pc

from mapper_fivetran import SystemColumns
from mapper_fivetran._util import transform_name

if t.TYPE_CHECKING:
    from singer_sdk.mapper import StreamMap

_SDC_EXTRACTED_AT = "_sdc_extracted_at"
_SDC_DELETED_AT = "_sdc_deleted_at"


class BatchFivetranIdError(ValueError):
    """Raised when a keyless stream is processed as an Arrow BATCH.

    `_fivetran_id` (an MD5 hash of the full record) cannot be computed for
    Arrow batches without a row-wise, unvectorized pass over every column, so
    BATCH mode requires `key_properties` instead of silently falling back to
    it.
    """

    def __init__(self, stream: str) -> None:
        """Initialize the error for the given stream name.

        Args:
            stream: The originating stream name.
        """
        super().__init__(
            f"stream {stream!r} has no key_properties: `_fivetran_id` is not "
            "computed for Arrow BATCH messages, so key_properties are required",
        )


def assert_batch_supported(stream: str, stream_map: StreamMap) -> None:
    """Raise `BatchFivetranIdError` if `stream_map` would need `_fivetran_id`.

    Args:
        stream: The originating stream name (for the error message).
        stream_map: The registered stream map for this stream.

    Raises:
        BatchFivetranIdError: If the stream was registered with no
            `key_properties`.
    """
    if SystemColumns.FIVETRAN_ID in (stream_map.transformed_key_properties or []):
        raise BatchFivetranIdError(stream)


def flatten_table(table: pa.Table, max_level: int) -> pa.Table:
    """Flatten top-level struct columns, up to `max_level` levels deep.

    Args:
        table: The table to flatten.
        max_level: The maximum recursion level (zero-based, exclusive).

    Returns:
        A new table with struct columns expanded into one column per field.
    """
    for _ in range(max_level):
        if not any(pa.types.is_struct(field.type) for field in table.schema):
            break
        table = table.flatten()
    return table


_JSON_ENCODER = msgspec.json.Encoder()

_COMPLEX_TYPE_CHECKS = (
    pa.types.is_struct,
    pa.types.is_list,
    pa.types.is_large_list,
    pa.types.is_fixed_size_list,
    pa.types.is_map,
)


def _is_json_extension(data_type: pa.DataType) -> bool:
    return (
        isinstance(data_type, pa.BaseExtensionType)
        and data_type.extension_name == "arrow.json"
    )


def stringify_complex_columns(table: pa.Table) -> pa.Table:
    """JSON-encode any column still struct-, list-, or map-typed.

    A Singer SCHEMA message property must be scalar, so anything
    `flatten_table` couldn't fully unpack (nesting past `max_level`, or a
    list/map column, which `flatten_table` never touches) is serialized to a
    JSON string here, matching
    `singer_sdk.helpers._flattening.flatten_record`'s behavior for the
    record-based transform. No pyarrow compute kernel exists for
    struct/list -> JSON, so this is a `to_pylist()` + `msgspec.json.encode()`
    pass -- msgspec rather than the stdlib `json` module since it's ~12-15x
    faster for this shape of data and already a transitive dependency via
    `singer-sdk[msgspec]`. Scoped only to the columns that actually need it;
    nulls are preserved as nulls, not the string `"null"`.

    `arrow.json` extension columns (`pa.json_()`) are handled separately:
    their storage is already valid JSON text, so they're cast to plain
    `pa.string()` instead of re-encoded.

    Args:
        table: The table to stringify complex columns for.

    Returns:
        A new table with any struct/list/map/json columns replaced by plain
        string columns.
    """
    for i, field in enumerate(table.schema):
        column: pa.Array | pa.ChunkedArray
        if _is_json_extension(field.type):
            column = pc.cast(table.column(i), pa.string())
            table = table.set_column(i, pa.field(field.name, pa.string()), column)
            continue

        if not any(check(field.type) for check in _COMPLEX_TYPE_CHECKS):
            continue

        encoded = [
            _JSON_ENCODER.encode(value).decode() if value is not None else None
            for value in table.column(i).to_pylist()
        ]
        column = pa.array(encoded, type=pa.string())

        table = table.set_column(i, pa.field(field.name, pa.string()), column)

    return table


def rename_columns(table: pa.Table) -> pa.Table:
    """Rename every column to its Fivetran-compatible snake_case equivalent.

    Args:
        table: The table to rename columns for.

    Returns:
        A new table with renamed columns.
    """
    return table.rename_columns([transform_name(name) for name in table.schema.names])


def _column_index(table: pa.Table, name: str) -> int | None:
    names = table.schema.names
    return names.index(name) if name in names else None


def with_fivetran_synced(table: pa.Table) -> pa.Table:
    """Append the `_fivetran_synced` column, vectorized.

    Copies `_sdc_extracted_at` when present (nulls stay null), otherwise fills
    the column with the current UTC timestamp.

    Args:
        table: The table to append the column to. Column names are assumed to
            already be normalized, e.g. via `rename_columns`.

    Returns:
        A new table with the `_fivetran_synced` column appended.
    """
    idx = _column_index(table, _SDC_EXTRACTED_AT)
    synced: pa.Array | pa.ChunkedArray
    if idx is not None:
        synced = pc.cast(table.column(idx), pa.string())
    else:
        now = datetime.now(timezone.utc).isoformat()
        synced = pa.array([now] * table.num_rows, type=pa.string())
    return table.append_column(
        pa.field(SystemColumns.FIVETRAN_SYNCED.value, pa.string()),
        synced,
    )


def with_fivetran_deleted(table: pa.Table) -> pa.Table:
    """Append the `_fivetran_deleted` column, vectorized.

    True wherever `_sdc_deleted_at` is present and non-empty; False when
    absent, null, or an empty string. False for every row when
    `_sdc_deleted_at` is not a column.

    Args:
        table: The table to append the column to. Column names are assumed to
            already be normalized, e.g. via `rename_columns`.

    Returns:
        A new table with the `_fivetran_deleted` column appended.
    """
    idx = _column_index(table, _SDC_DELETED_AT)
    deleted: pa.Array | pa.ChunkedArray
    if idx is None:
        deleted = pa.array([False] * table.num_rows, type=pa.bool_())
    else:
        column = table.column(idx)
        deleted = pc.is_valid(column)
        if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
            # `not_equal` yields null (not False) for null rows; `and_kleene`
            # treats a False `is_valid` as dominant over that null, so nulls
            # still resolve to `False` without needing a `fill_null` pass
            empty = pa.scalar("", type=column.type)
            deleted = pc.and_kleene(deleted, pc.not_equal(column, empty))  # ty:ignore[no-matching-overload]
    return table.append_column(
        pa.field(SystemColumns.FIVETRAN_DELETED.value, pa.bool_()),
        deleted,
    )


def transform_table(table: pa.Table, stream_map: StreamMap) -> pa.Table:
    """Apply the full set of Fivetran BATCH transforms to an Arrow table.

    Args:
        table: The table to transform.
        stream_map: The registered stream map to source flattening options,
            e.g. from `PluginMapper.stream_maps[stream_id]`.

    Returns:
        A new, transformed table.
    """
    if stream_map.flattening_enabled and stream_map.flattening_options is not None:
        table = flatten_table(table, stream_map.flattening_options.max_level)
        table = stringify_complex_columns(table)
    table = rename_columns(table)
    table = with_fivetran_synced(table)
    return with_fivetran_deleted(table)
