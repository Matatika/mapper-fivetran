from datetime import datetime

import pytest

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FIVETRAN_SYNCED, FIVETRAN_DELETED, FivetranStreamMap


def test_constant_matches_system_column():
    """FIVETRAN_SYNCED should match the enum value."""
    assert FIVETRAN_SYNCED == SystemColumns.FIVETRAN_SYNCED.value


def test_transform_adds_timestamp_column():
    """transform() should add an ISO timestamp in `FIVETRAN_SYNCED`."""
    stream_map = FivetranStreamMap(
        "animals",
        {"properties": {}},
        [],
    )

    out = stream_map.transform({"name": "Otis"})

    assert FIVETRAN_SYNCED in out

    timestamp = out[FIVETRAN_SYNCED]
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
    assert FIVETRAN_DELETED in out
    # expect default value false
    deleted = out[FIVETRAN_DELETED]
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
    assert '_SDC_DELETED_AT' in out
    # expect FIVETRAN_DELETED column true
    assert FIVETRAN_DELETED in out
    deleted = out[FIVETRAN_DELETED]
    assert deleted