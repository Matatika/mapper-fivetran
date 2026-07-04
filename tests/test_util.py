"""Tests for shared, dependency-light helpers."""

from __future__ import annotations

import sys
import uuid

from mapper_fivetran._util import new_uuid


def test_new_uuid_is_unique():
    assert new_uuid() != new_uuid()


def test_new_uuid_version():
    expected_version = 7 if sys.version_info >= (3, 14) else 4
    assert new_uuid().version == expected_version


def test_new_uuid_returns_uuid_instance():
    assert isinstance(new_uuid(), uuid.UUID)
