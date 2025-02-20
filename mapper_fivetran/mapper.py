"""Fivetran mapper class."""

from __future__ import annotations

import hashlib
import json
import typing as t

import humps
import singer_sdk.typing as th
from singer_sdk import _singerlib as singer
from singer_sdk.helpers._flattening import FlatteningOptions
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper import DefaultStreamMap, PluginMapper
from singer_sdk.mapper_base import InlineMapper
from typing_extensions import override

if t.TYPE_CHECKING:
    from pathlib import PurePath


FIVETRAN_ID = "_fivetran_id"
FIVETRAN_SYNCED = "_fivetran_synced"


class FivetranStreamMap(DefaultStreamMap):
    """Fivetran default stream map."""

    @override
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            *args,
            flattening_options=FlatteningOptions(max_level=1, separator="_"),
        )

        self._transform_schema(self.transformed_schema)
        self._transform_key_properties(self.transformed_key_properties)

    @override
    def transform(self, record):
        record: dict[str] = super().transform(record)

        for name in record.copy():
            record[self._transform_name(name)] = record.pop(name)

        if not self.transformed_key_properties:
            record[FIVETRAN_ID] = hashlib.md5(
                json.dumps(record).encode(),
                usedforsecurity=False,
            ).hexdigest()

        # consider whether we can instead use the `time_extracted` value of the current
        # `RECORD` message
        record[FIVETRAN_SYNCED] = utc_now().isoformat()

        return record

    @override
    def get_filter_result(self, record):
        return True

    def _transform_schema(self, schema: dict[str]) -> None:
        properties: dict[str] = schema["properties"]

        for name in properties.copy():
            properties[self._transform_name(name)] = properties.pop(name)

        if not self.transformed_key_properties:
            properties[FIVETRAN_ID] = th.StringType().to_dict()
            self.transformed_key_properties = [FIVETRAN_ID]

        properties[FIVETRAN_SYNCED] = th.DateTimeType().to_dict()

    @staticmethod
    def _transform_name(name: str) -> str:
        name = humps.decamelize(name)
        return name.replace(".", "_")

    @classmethod
    def _transform_key_properties(cls, key_properties: list[str]) -> None:
        for i, name in enumerate(key_properties):
            key_properties[i] = cls._transform_name(name)


class FivetranMapper(InlineMapper):
    """Sample mapper for Fivetran."""

    name = "mapper-fivetran"

    config_jsonschema = th.PropertiesList(
        # TODO: Replace or remove this example config based on your needs
        th.Property(
            "example_config",
            th.StringType,
            title="Example Configuration",
            description="An example config, replace or remove based on your needs.",
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
            plugin_config=self.config,
            logger=self.logger,
        )
        self.mapper.default_mapper_type = FivetranStreamMap

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
