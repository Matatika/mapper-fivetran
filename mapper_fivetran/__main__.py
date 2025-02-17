"""Fivetran entry point."""

from __future__ import annotations

from mapper_fivetran.mapper import FivetranMapper

FivetranMapper.cli()
