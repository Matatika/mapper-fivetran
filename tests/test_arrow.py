"""Tests for the vectorized Arrow BATCH transforms."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pytest
from singer_sdk.helpers._flattening import FlatteningOptions

from mapper_fivetran import SystemColumns
from mapper_fivetran.arrow import (
    BatchFivetranIdError,
    assert_batch_supported,
    flatten_table,
    rename_columns,
    transform_table,
    with_fivetran_deleted,
    with_fivetran_synced,
)
from mapper_fivetran.mapper import FivetranStreamMap


@pytest.fixture
def stream_map():
    return FivetranStreamMap(
        stream_alias="animals",
        raw_schema={"properties": {}},
        key_properties=["name"],
        flattening_options=FlatteningOptions(max_level=1, flattening_enabled=True),
    )


def test_flatten_table_expands_top_level_struct():
    table = pa.table(
        {
            "name": ["Otis"],
            "userInfo": pa.array(
                [{"firstName": "Bob"}],
                type=pa.struct([("firstName", pa.string())]),
            ),
        }
    )

    flattened = flatten_table(table, max_level=1)

    assert flattened.schema.names == ["name", "userInfo.firstName"]


def test_flatten_table_respects_max_level():
    table = pa.table(
        {
            "outer": pa.array(
                [{"inner": {"value": 1}}],
                type=pa.struct(
                    [("inner", pa.struct([("value", pa.int64())]))],
                ),
            ),
        }
    )

    flattened = flatten_table(table, max_level=1)

    assert flattened.schema.names == ["outer.inner"]
    assert pa.types.is_struct(flattened.schema.field("outer.inner").type)


def test_flatten_table_noop_without_structs():
    table = pa.table({"name": ["Otis"]})

    flattened = flatten_table(table, max_level=1)

    assert flattened.schema.names == ["name"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("camelCase", "camel_case"),
        ("PascalCase", "pascal_case"),
        ("userInfo.firstName", "user_info_first_name"),
        ("UPPER_CASE", "upper_case"),
    ],
)
def test_rename_columns(name, expected):
    table = pa.table({name: [1]})

    renamed = rename_columns(table)

    assert renamed.schema.names == [expected]


def test_with_fivetran_synced_copies_sdc_extracted_at():
    table = pa.table(
        {
            "name": ["Otis"],
            "_sdc_extracted_at": ["2024-01-01T00:00:00+00:00"],
        }
    )

    result = with_fivetran_synced(table)

    assert result.column(SystemColumns.FIVETRAN_SYNCED.value).to_pylist() == [
        "2024-01-01T00:00:00+00:00"
    ]


def test_with_fivetran_synced_null_stays_null():
    table = pa.table(
        {
            "name": ["Otis"],
            "_sdc_extracted_at": pa.array([None], type=pa.string()),
        }
    )

    result = with_fivetran_synced(table)

    assert result.column(SystemColumns.FIVETRAN_SYNCED.value).to_pylist() == [None]


def test_with_fivetran_synced_defaults_to_now_when_absent():
    table = pa.table({"name": ["Otis"]})

    result = with_fivetran_synced(table)

    synced = result.column(SystemColumns.FIVETRAN_SYNCED.value)[0].as_py()
    assert datetime.fromisoformat(synced)
    assert datetime.fromisoformat(synced).tzinfo is not None


def test_with_fivetran_deleted_true_when_sdc_deleted_at_present():
    table = pa.table(
        {
            "name": ["Otis"],
            "_sdc_deleted_at": ["2024-01-01T00:00:00+00:00"],
        }
    )

    result = with_fivetran_deleted(table)

    assert result.column(SystemColumns.FIVETRAN_DELETED.value).to_pylist() == [True]


def test_with_fivetran_deleted_false_when_sdc_deleted_at_null():
    table = pa.table(
        {
            "name": ["Otis"],
            "_sdc_deleted_at": pa.array([None], type=pa.string()),
        }
    )

    result = with_fivetran_deleted(table)

    assert result.column(SystemColumns.FIVETRAN_DELETED.value).to_pylist() == [False]


def test_with_fivetran_deleted_false_when_absent():
    table = pa.table({"name": ["Otis"]})

    result = with_fivetran_deleted(table)

    assert result.column(SystemColumns.FIVETRAN_DELETED.value).to_pylist() == [False]


def test_assert_batch_supported_raises_when_no_key_properties():
    stream_map = FivetranStreamMap(
        stream_alias="animals",
        raw_schema={"properties": {}},
        key_properties=[],
        flattening_options=None,
    )

    with pytest.raises(BatchFivetranIdError):
        assert_batch_supported("animals", stream_map)


def test_assert_batch_supported_passes_with_key_properties(stream_map):
    assert_batch_supported("animals", stream_map)


def test_transform_table_end_to_end(stream_map):
    table = pa.table(
        {
            "name": ["Otis"],
            "userInfo": pa.array(
                [{"firstName": "Bob"}],
                type=pa.struct([("firstName", pa.string())]),
            ),
            "_sdc_extracted_at": ["2024-01-01T00:00:00+00:00"],
        }
    )

    result = transform_table(table, stream_map)

    assert result.schema.names == [
        "name",
        "user_info_first_name",
        "_sdc_extracted_at",
        SystemColumns.FIVETRAN_SYNCED.value,
        SystemColumns.FIVETRAN_DELETED.value,
    ]
    assert result.column(SystemColumns.FIVETRAN_SYNCED.value).to_pylist() == [
        "2024-01-01T00:00:00+00:00"
    ]
    assert result.column(SystemColumns.FIVETRAN_DELETED.value).to_pylist() == [False]
