from __future__ import annotations

from typing import Any

import ckan.plugins.toolkit as tk

from ckanext.federated_index import shared


def get_validators():
    return {
        "federated_index_profile": federated_index_profile,
    }


def federated_index_profile(value: Any) -> shared.Profile:
    if isinstance(value, shared.Profile):
        return value

    if profile := shared.get_profile(value):
        return profile

    msg = "Profile does not exist"
    raise tk.Invalid(msg)
