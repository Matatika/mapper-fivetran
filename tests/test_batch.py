"""Tests for `FivetranMapper.map_batch_message`."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import ipc

from mapper_fivetran import SystemColumns
from mapper_fivetran.arrow import BatchFivetranIdError
from mapper_fivetran.mapper import FivetranMapper


def _write_arrow_file(path: str, table: pa.Table) -> str:
    with ipc.new_file(path, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return f"file://{path}"


def _read_arrow_file(uri: str) -> pa.Table:
    with ipc.open_file(uri.removeprefix("file://")) as reader:
        return reader.read_all()


@pytest.fixture
def mapper(tmp_path):
    return FivetranMapper(
        config={"batch_root_dir": str(tmp_path / "out")},
        validate_config=False,
    )


def _register_schema(mapper: FivetranMapper, key_properties: list[str]) -> None:
    list(
        mapper.map_schema_message(
            {
                "type": "SCHEMA",
                "stream": "animals",
                "schema": {"properties": {"name": {"type": "string"}}},
                "key_properties": key_properties,
            }
        )
    )


def test_map_batch_message_transforms_columns(mapper: FivetranMapper, tmp_path):
    _register_schema(mapper, key_properties=["name"])

    src = _write_arrow_file(
        str(tmp_path / "src.arrow"),
        pa.table(
            {"name": ["Otis"], "_sdc_extracted_at": ["2024-01-01T00:00:00+00:00"]}
        ),
    )

    (out_message,) = list(
        mapper.map_batch_message(
            {
                "type": "BATCH",
                "stream": "animals",
                "encoding": {"format": "arrow"},
                "manifest": [src],
            }
        )
    )
    out = out_message.to_dict()

    assert out["type"] == "BATCH"
    assert out["stream"] == "animals"
    assert len(out["manifest"]) == 1

    result = _read_arrow_file(out["manifest"][0])
    assert result.schema.names == [
        "name",
        "_sdc_extracted_at",
        SystemColumns.FIVETRAN_SYNCED.value,
        SystemColumns.FIVETRAN_DELETED.value,
    ]
    assert result.column("name").to_pylist() == ["Otis"]
    assert result.column(SystemColumns.FIVETRAN_SYNCED.value).to_pylist() == [
        "2024-01-01T00:00:00+00:00"
    ]
    assert result.column(SystemColumns.FIVETRAN_DELETED.value).to_pylist() == [False]


def test_map_batch_message_raises_without_key_properties(
    mapper: FivetranMapper, tmp_path
):
    _register_schema(mapper, key_properties=[])

    src = _write_arrow_file(str(tmp_path / "src.arrow"), pa.table({"name": ["Otis"]}))

    with pytest.raises(BatchFivetranIdError):
        list(
            mapper.map_batch_message(
                {
                    "type": "BATCH",
                    "stream": "animals",
                    "encoding": {"format": "arrow"},
                    "manifest": [src],
                }
            )
        )


def test_map_batch_message_rejects_non_arrow_encoding(mapper: FivetranMapper):
    _register_schema(mapper, key_properties=["name"])

    with pytest.raises(ValueError, match="arrow"):
        list(
            mapper.map_batch_message(
                {
                    "type": "BATCH",
                    "stream": "animals",
                    "encoding": {"format": "jsonl"},
                    "manifest": [],
                }
            )
        )


def test_map_batch_message_writes_to_configured_dir(mapper: FivetranMapper, tmp_path):
    _register_schema(mapper, key_properties=["name"])

    src = _write_arrow_file(str(tmp_path / "src.arrow"), pa.table({"name": ["Otis"]}))

    (out_message,) = list(
        mapper.map_batch_message(
            {
                "type": "BATCH",
                "stream": "animals",
                "encoding": {"format": "arrow"},
                "manifest": [src],
            }
        )
    )
    out_path = out_message.to_dict()["manifest"][0].removeprefix("file://")

    assert Path(out_path).parent == tmp_path / "out"


def test_map_batch_message_deletes_source_files(mapper: FivetranMapper, tmp_path):
    _register_schema(mapper, key_properties=["name"])

    src_path = tmp_path / "src.arrow"
    src = _write_arrow_file(str(src_path), pa.table({"name": ["Otis"]}))
    assert src_path.exists()

    list(
        mapper.map_batch_message(
            {
                "type": "BATCH",
                "stream": "animals",
                "encoding": {"format": "arrow"},
                "manifest": [src],
            }
        )
    )

    assert not src_path.exists()


def test_map_batch_message_leaves_output_files_for_downstream(
    mapper: FivetranMapper, tmp_path
):
    _register_schema(mapper, key_properties=["name"])

    src = _write_arrow_file(str(tmp_path / "src.arrow"), pa.table({"name": ["Otis"]}))

    (out_message,) = list(
        mapper.map_batch_message(
            {
                "type": "BATCH",
                "stream": "animals",
                "encoding": {"format": "arrow"},
                "manifest": [src],
            }
        )
    )
    out_path = out_message.to_dict()["manifest"][0].removeprefix("file://")

    assert Path(out_path).exists()
