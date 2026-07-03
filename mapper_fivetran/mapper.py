"""Fivetran mapper class."""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import tempfile
import typing as t
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

import singer_sdk.typing as th
from pyarrow import ipc
from singer_sdk import singerlib as singer
from singer_sdk.contrib.msgspec import MsgSpecReader, MsgSpecWriter
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._flattening import FlatteningOptions, flatten_record
from singer_sdk.helpers._util import utc_now
from singer_sdk.helpers.capabilities import PluginCapabilities
from singer_sdk.mapper import DefaultStreamMap, PluginMapper
from singer_sdk.mapper_base import InlineMapper
from singer_sdk.singerlib.encoding.base import SingerMessageType
from typing_extensions import override

from mapper_fivetran import SystemColumns
from mapper_fivetran._util import new_uuid, transform_name
from mapper_fivetran.arrow import assert_batch_supported, transform_table

if t.TYPE_CHECKING:
    from pathlib import PurePath


_SDC_EXTRACTED_AT = "_sdc_extracted_at"
_SDC_DELETED_AT = "_sdc_deleted_at"
_SUPPORTED_BATCH_FORMAT = "arrow"


class FivetranStreamMap(DefaultStreamMap):
    """Fivetran default stream map."""

    @override
    def __init__(
        self,
        stream_alias,
        raw_schema,
        key_properties,
        flattening_options,
    ) -> None:
        flattening_options = flattening_options and FlatteningOptions(
            max_level=flattening_options.max_level,
            flattening_enabled=flattening_options.flattening_enabled,
            separator="_",  # override default separator
        )

        super().__init__(stream_alias, raw_schema, key_properties, flattening_options)

        # preserve flattened schema
        self.flattened_schema = copy.deepcopy(self.transformed_schema)

        self._apply_key_property_transformations()
        self._apply_schema_transformations()

    @override
    def flatten_record(self, record):
        if (
            not self.flattening_options
            or not self.flattening_enabled
            or not self.records_require_flattening
        ):
            return record

        # reference flattened schema specifically for record lookup, as other
        # transformations are applied to `self.transformed_schema` that affect
        # the property key in schema checks
        return flatten_record(
            record,
            flattened_schema=self.flattened_schema,
            max_level=self.flattening_options.max_level,
            separator=self.flattening_options.separator,
        )

    @override
    def transform(self, record):
        record: dict[str] = super().transform(record)

        for name in record.copy():
            record[self._transform_name(name)] = record.pop(name)

        if SystemColumns.FIVETRAN_ID in self.transformed_key_properties:
            record[SystemColumns.FIVETRAN_ID] = hashlib.md5(
                json.dumps(record, default=str).encode(),
                usedforsecurity=False,
            ).hexdigest()

        # `_transform_name` lowercases every key, so the SDC columns can be looked
        # up directly without building a lowercased copy of the whole record.
        record[SystemColumns.FIVETRAN_SYNCED] = record.get(
            _SDC_EXTRACTED_AT, utc_now().isoformat()
        )
        record[SystemColumns.FIVETRAN_DELETED] = bool(record.get(_SDC_DELETED_AT))

        return record

    @override
    def get_filter_result(self, record):
        return True

    def _apply_schema_transformations(self):
        properties: dict[str] = self.transformed_schema["properties"]

        for name in properties.copy():
            properties[self._transform_name(name)] = properties.pop(name)

        if SystemColumns.FIVETRAN_ID in self.transformed_key_properties:
            properties[SystemColumns.FIVETRAN_ID] = th.StringType().to_dict()

        properties[SystemColumns.FIVETRAN_SYNCED] = th.DateTimeType().to_dict()
        properties[SystemColumns.FIVETRAN_DELETED] = th.BooleanType().to_dict()

    @functools.cached_property
    def records_require_flattening(self) -> bool:
        """Whether any property could be expanded or json-dumped by flattening."""
        # Checked on the raw schema: flatten_schema rewrites object/array
        # properties to scalar types, so they no longer show as complex in the
        # flattened schema.
        for prop in self.raw_schema["properties"].values():
            # A property with no "type" (anyOf/oneOf/$ref) is treated as scalar.
            type_ = prop.get("type", ())

            if "object" in type_:
                # An object with explicitly empty `properties` ({}) is dropped by
                # flatten_schema, so needs no flattening. An *opaque* object (no
                # `properties` key) is kept as a json-dumped string and does --
                # hence `!= {}` rather than truthiness (a missing key is
                # `None != {}`).
                return prop.get("properties") != {}

            if "array" in type_:
                return True

        return False

    @staticmethod
    def _transform_name(name: str) -> str:
        # memoized in `transform_name` itself, since the Arrow BATCH path
        # (`mapper_fivetran.arrow.rename_columns`) calls it directly too
        return transform_name(name)

    def _apply_key_property_transformations(self):
        if not self.transformed_key_properties:
            self.transformed_key_properties = [SystemColumns.FIVETRAN_ID]
            return

        for i, name in enumerate(self.transformed_key_properties):
            self.transformed_key_properties[i] = self._transform_name(name)


@dataclass
class BatchMessage(singer.Message):
    """Singer BATCH message.

    `singer_sdk.singerlib` has no `BatchMessage` class as of 0.48.
    """

    stream: str
    encoding: dict
    manifest: list[str]

    def __post_init__(self) -> None:
        """Set the message type."""
        self.type = SingerMessageType.BATCH


class FivetranMapper(InlineMapper):
    """Sample mapper for Fivetran."""

    name = "mapper-fivetran"

    # use msgspec for (de)serialization instead of the default json/simplejson,
    # which is significantly faster on the per-record read/write hot path
    message_reader_class = MsgSpecReader
    message_writer_class = MsgSpecWriter

    config_jsonschema = th.PropertiesList(
        th.Property(
            "batch_root_dir",
            th.StringType,
            title="Batch Output Directory",
            description=(
                "Directory to write transformed Arrow IPC BATCH files to. "
                "Defaults to a fresh temporary directory."
            ),
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.mapper = PluginMapper(
            plugin_config={
                "flattening_enabled": True,
                "flattening_max_depth": 1,
                **self.config,
            },
            logger=self.logger,
        )
        self.mapper.default_mapper_type = FivetranStreamMap

    @override
    @classproperty
    def capabilities(self):
        return [
            PluginCapabilities.FLATTENING,
            PluginCapabilities.BATCH,
        ]

    @cached_property
    def _batch_output_dir(self) -> Path:
        # cached so repeated BATCH messages share one directory instead of each
        # getting its own fresh `tempfile.mkdtemp()` result
        directory = Path(
            self.config.get("batch_root_dir")
            or tempfile.mkdtemp(prefix="mapper-fivetran-"),
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def map_schema_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})

        stream_id: str = message_dict["stream"]
        self.mapper.register_raw_stream_schema(
            stream_id,
            message_dict["schema"],
            message_dict.get("key_properties", []),
        )
        for stream_map in self.mapper.stream_maps[stream_id]:
            yield singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                message_dict.get("bookmark_keys", []),
            )

    def map_record_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.RecordMessage]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_id: str = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_id]:
            mapped_record = stream_map.transform(message_dict["record"])
            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=message_dict.get("version"),
                    time_extracted=utc_now(),
                )

    def map_batch_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a batch message to zero or more new messages.

        Applies the same transforms as `map_record_message`, but vectorized
        over whole Arrow IPC files instead of individual records. Unlike the
        record path, `_fivetran_id` is not computed for BATCH messages: see
        `mapper_fivetran.arrow.assert_batch_supported`.

        Source files listed in the incoming manifest are deleted once fully
        read. Output files are left for the downstream consumer to clean up.

        Args:
            message_dict: A BATCH message JSON dictionary.

        Raises:
            ValueError: If the batch encoding format is not 'arrow'.
        """
        self._assert_line_requires(
            message_dict, requires={"stream", "encoding", "manifest"}
        )

        stream_id: str = message_dict["stream"]
        encoding: dict = message_dict["encoding"]
        if encoding.get("format") != _SUPPORTED_BATCH_FORMAT:
            msg = (
                f"mapper-fivetran only supports {_SUPPORTED_BATCH_FORMAT!r} BATCH "
                f"encoding, got {encoding.get('format')!r}"
            )
            raise ValueError(msg)

        tables = []
        for file_uri in message_dict["manifest"]:
            src_path = Path(file_uri.removeprefix("file://"))
            with ipc.open_file(str(src_path)) as reader:
                tables.append(reader.read_all())
            # the mapper is the sole consumer of source batch files, so it's
            # safe to remove them once fully read; output files are left for
            # the downstream consumer (e.g. a target) to clean up
            src_path.unlink()

        output_dir = self._batch_output_dir

        for stream_map in self.mapper.stream_maps[stream_id]:
            assert_batch_supported(stream_id, stream_map)

            new_manifest = []
            for i, table in enumerate(tables):
                transformed = transform_table(table, stream_map)

                out_path = (
                    output_dir / f"{stream_map.stream_alias}_{new_uuid().hex}_{i}.arrow"
                )
                with ipc.new_file(str(out_path), transformed.schema) as writer:
                    for batch in transformed.to_batches():
                        writer.write_batch(batch)

                new_manifest.append(f"file://{out_path}")

            yield BatchMessage(
                stream=stream_map.stream_alias,
                manifest=new_manifest,
                encoding=encoding,
            )

    def map_state_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        yield singer.StateMessage.from_dict(message_dict)

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        yield singer.ActivateVersionMessage.from_dict(message_dict)


if __name__ == "__main__":
    FivetranMapper.cli()
