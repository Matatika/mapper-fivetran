from datetime import datetime

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FivetranStreamMap


def test_transform_adds_timestamp_column():
    """transform() should add an ISO timestamp in `FIVETRAN_SYNCED`."""
    stream_map = FivetranStreamMap(
        "animals",
        {"properties": {}},
        [],
    )

    out = stream_map.transform({"name": "Otis"})

    assert SystemColumns.FIVETRAN_SYNCED in out

    timestamp = out[SystemColumns.FIVETRAN_SYNCED]
    parsed = datetime.fromisoformat(timestamp)

    assert parsed.isoformat() == timestamp
    assert parsed.tzinfo is not None


def test_transform_adds_deleted_column():
    """transform() should add a boolean value in `FIVETRAN_DELETED`."""
    stream_map = FivetranStreamMap(
        "animals",
        {"properties": {}},
        [],
    )

    # given a simple record
    out = stream_map.transform({"name": "Otis"})
    # expect FIVETRAN_DELETED column has been added
    assert SystemColumns.FIVETRAN_DELETED in out
    # expect default value false
    deleted = out[SystemColumns.FIVETRAN_DELETED]
    assert not deleted


def test_transform_sdc_deleted_at_deleted_column():
    """transform() should set boolean value of `FIVETRAN_DELETED` when `_SDC_DELETED_AT`."""
    stream_map = FivetranStreamMap(
        "animals",
        {"properties": {}},
        [],
    )

    # given a simple record
    out = stream_map.transform({"name": "Otis", "_SDC_DELETED_AT": datetime.now()})
    # expect _SDC_DELETED_AT still in results
    assert "_SDC_DELETED_AT" in out
    # expect FIVETRAN_DELETED column true
    assert SystemColumns.FIVETRAN_DELETED in out
    deleted = out[SystemColumns.FIVETRAN_DELETED]
    assert deleted
