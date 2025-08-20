"""Fivetran mapper class."""

from __future__ import annotations

import copy
import hashlib
import json
import typing as t

import humps
import singer_sdk.typing as th
from singer_sdk import singerlib as singer
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._flattening import FlatteningOptions, flatten_record
from singer_sdk.helpers._util import utc_now
from singer_sdk.helpers.capabilities import PluginCapabilities
from singer_sdk.mapper import DefaultStreamMap, PluginMapper
from singer_sdk.mapper_base import InlineMapper
from typing_extensions import override

from mapper_fivetran import SystemColumns

if t.TYPE_CHECKING:
    from pathlib import PurePath


_SDC_EXTRACTED_AT = "_sdc_extracted_at"
_SDC_DELETED_AT = "_sdc_deleted_at"


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
        if not self.flattening_options or not self.flattening_enabled:
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

        record_lower_keys = {k.lower(): v for k, v in record.items()}

        record[SystemColumns.FIVETRAN_SYNCED] = record_lower_keys.get(
            _SDC_EXTRACTED_AT, utc_now().isoformat()
        )
        record[SystemColumns.FIVETRAN_DELETED] = bool(
            record_lower_keys.get(_SDC_DELETED_AT)
        )

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

    @staticmethod
    def _transform_name(name: str) -> str:
        # handle names with mixed casing, underscores and capital subsequences
        transformed_parts = [
            part.lower() if part.isupper() else humps.decamelize(humps.camelize(part))
            for part in name.split("_")
        ]

        transformed = "_".join(transformed_parts)
        return transformed.replace(".", "_")

    def _apply_key_property_transformations(self):
        if not self.transformed_key_properties:
            self.transformed_key_properties = [SystemColumns.FIVETRAN_ID]
            return

        for i, name in enumerate(self.transformed_key_properties):
            self.transformed_key_properties[i] = self._transform_name(name)


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
        ]

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
