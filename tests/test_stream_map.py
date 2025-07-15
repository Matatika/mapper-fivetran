from datetime import datetime, timezone

import pytest

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FivetranStreamMap


@pytest.fixture
def stream_map():
    return FivetranStreamMap(
        "animals",
        {"properties": {}},
        [],
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
