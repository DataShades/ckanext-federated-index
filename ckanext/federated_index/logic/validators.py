from __future__ import annotations

from typing import Any

import ckan.plugins.toolkit as tk

from ckanext.federated_index import shared


def federated_index_profile(value: Any) -> shared.Profile:
    """Convert profile ID into profile object."""
    if isinstance(value, shared.Profile):
        return value

    if profile := shared.get_profile(value):
        return profile

    msg = "Profile does not exist"
    raise tk.Invalid(msg)
