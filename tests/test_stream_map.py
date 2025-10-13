"""Tests mapper stream map."""

from datetime import datetime, timezone

import pytest

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FivetranStreamMap


@pytest.fixture
def stream_map():
    return FivetranStreamMap(
        stream_alias="animals",
        raw_schema={"properties": {}},
        key_properties=[],
        flattening_options=None,
    )


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
