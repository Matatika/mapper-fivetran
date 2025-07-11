from datetime import datetime

import pytest

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FIVETRAN_SYNCED, FivetranStreamMap


def test_constant_matches_system_column():
    """FIVETRAN_SYNCED should match the enum value."""
    assert FIVETRAN_SYNCED == SystemColumns.FIVETRAN_SYNCED.value


def test_transform_adds_timestamp_column():
    """transform() should add an ISO timestamp in `FIVETRAN_SYNCED`."""
    stream_map = FivetranStreamMap(
        stream_alias="animals",
        schema={"properties": {}},
        key_properties=[],
    )

    out = stream_map.transform({"name": "Otis"})

    assert FIVETRAN_SYNCED in out

    timestamp = out[FIVETRAN_SYNCED]
    parsed = datetime.fromisoformat(timestamp)

    assert parsed.isoformat() == timestamp
    assert parsed.tzinfo is not None
