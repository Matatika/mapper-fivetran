"""Fivetran Mapper."""

from enum import Enum


class SystemColumns(str, Enum):
    """Fivetran system columns.

    https://fivetran.com/docs/core-concepts/system-columns-and-tables#systemcolumns
    """

    FIVETRAN_ID = "_fivetran_id"
    FIVETRAN_SYNCED = "_fivetran_synced"
    FIVETRAN_DELETED = "_fivetran_deleted"


SYSTEM_COLUMN_VALUES = [c.value for c in SystemColumns]
