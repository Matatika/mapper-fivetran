import ast
import inspect

import pytest

from mapper_fivetran import SystemColumns
from mapper_fivetran.mapper import FIVETRAN_SYNCED, FivetranStreamMap


def test_constant_matches_system_column():
    """FIVETRAN_SYNCED should match the enum value."""
    assert FIVETRAN_SYNCED == SystemColumns.FIVETRAN_SYNCED.value


def test_transform_assigns_iso_timestamp():
    """Verify `transform` assigns an ISO8601 timestamp to the synced column."""
    source = inspect.getsource(FivetranStreamMap.transform)
    tree = ast.parse(source)
    found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "record"
            ):
                slice_node = target.slice
                if isinstance(slice_node, ast.Name) and slice_node.id == "FIVETRAN_SYNCED":
                    if (
                        isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Attribute)
                        and node.value.func.attr == "isoformat"
                        and isinstance(node.value.func.value, ast.Call)
                        and isinstance(node.value.func.value.func, ast.Name)
                        and node.value.func.value.func.id == "utc_now"
                    ):
                        found = True
                        break
    assert found, "transform should assign utc_now().isoformat() to FIVETRAN_SYNCED"
