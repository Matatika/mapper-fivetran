"""Tests mapper stream map."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from singer_sdk.helpers._flattening import FlatteningOptions

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FivetranStreamMap


@pytest.fixture
def make_stream_map():
    def _make_stream_map(properties: dict | None = None) -> FivetranStreamMap:
        return FivetranStreamMap(
            stream_alias="animals",
            raw_schema={"properties": properties or {}},
            key_properties=[],
            flattening_options=FlatteningOptions(
                max_level=1,
                flattening_enabled=True,
                separator="_",
            ),
        )

    return _make_stream_map


@pytest.fixture
def stream_map(make_stream_map):
    return make_stream_map()

@pytest.mark.parametrize(
    ("properties", "requires_flattening"),
    [
        pytest.param(
            {"id": {"type": "integer"}, "name": {"type": "string"}},
            False,
            id="all scalar",
        ),
        pytest.param({"tags": {"type": "array"}}, True, id="array"),
        pytest.param({"meta": {"type": "object"}}, True, id="bare object"),
        pytest.param(
            {"obj": {"type": "object", "properties": {"x": {"type": "integer"}}}},
            True,
            id="nested object",
        ),
        pytest.param(
            {"meta": {"type": ["null", "object"]}},
            True,
            id="nullable object",
        ),
        pytest.param(
            {"tags": {"type": ["null", "array"]}},
            True,
            id="nullable array",
        ),
        pytest.param(
            # anyOf/oneOf/$ref have no top-level "type"; must not raise
            {"id": {"type": "integer"}, "val": {"anyOf": [{"type": "object"}]}},
            False,
            id="property without type",
        ),
    ],
)
def test_requires_flattening(make_stream_map, properties, requires_flattening):
    assert make_stream_map(properties).requires_flattening is requires_flattening


@pytest.mark.parametrize(
    ("properties", "record", "expected"),
    [
        pytest.param(
            {"id": {"type": "integer"}},
            {"id": 1, "name": "Otis", "weight": None},
            {"id": 1, "name": "Otis", "weight": None},
            id="flat record passthrough",
        ),
        pytest.param(
            {
                "id": {"type": "integer"},
                "obj": {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
                },
            },
            {"id": 1, "obj": {"x": 1, "y": 2}},
            {"id": 1, "obj_x": 1, "obj_y": 2},
            id="nested object expands",
        ),
        pytest.param(
            {"id": {"type": "integer"}, "tags": {"type": "array"}},
            {"id": 1, "tags": [1, 2, 3]},
            {"id": 1, "tags": "[1,2,3]"},
            id="nested array jsondumped",
        ),
        pytest.param(
            {
                "id": {"type": "integer"},
                "obj": {"type": "object", "properties": {"x": {"type": "object"}}},
            },
            {"id": 1, "obj": {"x": {"deep": 1}}},
            {"id": 1, "obj_x": '{"deep":1}'},
            id="deep nesting jsondumped",
        ),
    ],
)
def test_flatten_record(make_stream_map, properties, record, expected):
    assert make_stream_map(properties).flatten_record(record) == expected


def test_flatten_skips_nesting_under_undeclared_columns(make_stream_map):
    """Pass through nesting under columns the schema doesn't declare.

    A value nesting under a column the schema doesn't declare as object/array is
    passed through unchanged: the gate trusts the schema, and such a field isn't
    in the schema so a SQL target drops it anyway.
    """
    record = {"id": 1, "obj": {"x": 1}}

    assert make_stream_map({"id": {"type": "integer"}}).flatten_record(record) == record


def test_transform_no__sdc_extracted_at(stream_map: FivetranStreamMap):
    transformed_record = stream_map.transform({"name": "Otis"})

    assert SystemColumns.FIVETRAN_SYNCED in transformed_record
    assert datetime.fromisoformat(transformed_record[SystemColumns.FIVETRAN_SYNCED])


@pytest.mark.parametrize(
    "column_name",
    ["_sdc_extracted_at", "_SDC_EXTRACTED_AT"],
)
def test_transform__sdc_extracted_at(stream_map: FivetranStreamMap, column_name):
    transformed_record = stream_map.transform(
        {
            "name": "Otis",
            column_name: datetime.now(tz=timezone.utc).isoformat(),
        }
    )

    assert SystemColumns.FIVETRAN_SYNCED in transformed_record
    assert datetime.fromisoformat(transformed_record[SystemColumns.FIVETRAN_SYNCED])


@pytest.mark.parametrize(
    "column_name",
    ["_sdc_extracted_at", "_SDC_EXTRACTED_AT"],
)
def test_transform__sdc_extracted_at_null(stream_map: FivetranStreamMap, column_name):
    transformed_record = stream_map.transform({"name": "Otis", column_name: None})

    assert SystemColumns.FIVETRAN_SYNCED in transformed_record
    assert transformed_record[SystemColumns.FIVETRAN_SYNCED] is None


def test_transform_no__sdc_deleted_at(stream_map: FivetranStreamMap):
    transformed_record = stream_map.transform({"name": "Otis"})

    assert SystemColumns.FIVETRAN_DELETED in transformed_record
    assert transformed_record[SystemColumns.FIVETRAN_DELETED] is False


@pytest.mark.parametrize(
    "column_name",
    ["_sdc_deleted_at", "_SDC_DELETED_AT"],
)
def test_transform__sdc_deleted_at(stream_map: FivetranStreamMap, column_name):
    transformed_record = stream_map.transform(
        {
            "name": "Otis",
            column_name: datetime.now(tz=timezone.utc).isoformat(),
        }
    )

    assert SystemColumns.FIVETRAN_DELETED in transformed_record
    assert transformed_record[SystemColumns.FIVETRAN_DELETED] is True


@pytest.mark.parametrize(
    "column_name",
    ["_sdc_deleted_at", "_SDC_DELETED_AT"],
)
def test_transform__sdc_deleted_at_null(stream_map: FivetranStreamMap, column_name):
    transformed_record = stream_map.transform({"name": "Otis", column_name: None})

    assert SystemColumns.FIVETRAN_DELETED in transformed_record
    assert transformed_record[SystemColumns.FIVETRAN_DELETED] is False


@pytest.mark.parametrize(
    ("name", "expected_transformed_name"),
    [
        pytest.param(
            "snake_case",
            "snake_case",
            id="snake case",
        ),
        pytest.param(
            "camelCase",
            "camel_case",
            id="camel case",
        ),
        pytest.param(
            "PascalCase",
            "pascal_case",
            id="pascal case",
        ),
        pytest.param(
            "kebab-case",
            "kebab_case",
            id="kebab case",
        ),
        pytest.param(
            "snake_case_camelCase",
            "snake_case_camel_case",
            id="mixed snake/camel case",
        ),
        pytest.param(
            "snake_case_PascalCase",
            "snake_case_pascal_case",
            id="mixed snake/pascal case",
        ),
        pytest.param(
            "snake_case_kebab-case",
            "snake_case_kebab_case",
            id="mixed snake/kebab case",
        ),
        pytest.param(
            "snake_case.with_dot_separator",
            "snake_case_with_dot_separator",
            id="snake case with dot separator",
        ),
        pytest.param(
            "UPPERCASE",
            "uppercase",
            id="upper case",
        ),
        pytest.param(
            "UPPER_CASE",
            "upper_case",
            id="upper snake case",
        ),
        pytest.param(
            "UPPER_CASE_CamelCase",
            "upper_case_camel_case",
            id="mixed upper snake/camel case",
        ),
        pytest.param(
            "Snake_Case",
            "snake_case",
            id="title snake case",
        ),
        pytest.param(
            "IPAddress",
            "ip_address",
            id="pascal case with multiple leading capitals",
        ),
    ],
)
def test_transform_name(name, expected_transformed_name):
    actual_transformed_name = FivetranStreamMap._transform_name(name)
    assert expected_transformed_name == actual_transformed_name
