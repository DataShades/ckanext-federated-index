from __future__ import annotations

import click

__all__ = [
    "federated_index",
]


@click.group()
def federated_index():
    """Federate datasets from the remote portal"""
