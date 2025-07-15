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


def test_transform_adds_timestamp_column(stream_map: FivetranStreamMap):
    """transform() should add an ISO timestamp in `FIVETRAN_SYNCED`."""
    out = stream_map.transform({"name": "Otis"})

    assert SystemColumns.FIVETRAN_SYNCED in out

    timestamp = out[SystemColumns.FIVETRAN_SYNCED]
    parsed = datetime.fromisoformat(timestamp)

    assert parsed.isoformat() == timestamp
    assert parsed.tzinfo is not None

@pytest.mark.parametrize(
    "column_name",
    ["_sdc_extracted_at", "_SDC_EXTRACTED_AT"],
)
def test_transform__sdc_extracted_at_adds_timestamp_column(
    stream_map: FivetranStreamMap,
    column_name,
):
    """transform() should add an ISO timestamp in `FIVETRAN_SYNCED` when `_SDC_EXTRACTED_AT`."""
    out = stream_map.transform(
        {
            "name": "Otis",
            column_name: datetime.now(tz=timezone.utc).isoformat(),
        }
    )

    assert SystemColumns.FIVETRAN_SYNCED in out

    timestamp = out[SystemColumns.FIVETRAN_SYNCED]
    parsed = datetime.fromisoformat(timestamp)

    assert parsed.isoformat() == timestamp
    assert parsed.tzinfo is not None


def test_transform_adds_deleted_column(stream_map: FivetranStreamMap):
    """transform() should add a boolean value in `FIVETRAN_DELETED`."""
    # given a simple record
    out = stream_map.transform({"name": "Otis"})
    # expect FIVETRAN_DELETED column has been added
    assert SystemColumns.FIVETRAN_DELETED in out
    # expect default value false
    deleted = out[SystemColumns.FIVETRAN_DELETED]
    assert not deleted


@pytest.mark.parametrize(
    "column_name",
    ["_sdc_deleted_at", "_SDC_DELETED_AT"],
)
def test_transform_sdc_deleted_at_deleted_column(
    stream_map: FivetranStreamMap,
    column_name,
):
    """transform() should set boolean value of `FIVETRAN_DELETED` when `_SDC_DELETED_AT`."""
    # given a simple record
    out = stream_map.transform(
        {
            "name": "Otis",
            column_name: datetime.now(tz=timezone.utc).isoformat(),
        }
    )

    # expect _SDC_DELETED_AT still in results
    assert column_name in out
    # expect FIVETRAN_DELETED column true
    assert SystemColumns.FIVETRAN_DELETED in out
    deleted = out[SystemColumns.FIVETRAN_DELETED]
    assert deleted
