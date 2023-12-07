from __future__ import annotations

from typing import Any

from ckan import authz


def federated_index_access(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return {"success": False}


def federated_index_profile_refresh(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_list(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_index(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_clear(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_remove(
    context: Any,
    data_dict: dict[str, Any],
) -> Any:
    return authz.is_authorized("federated_index_access", context, data_dict)
