from __future__ import annotations

from typing import Any

from ckan import authz, types


def federated_index_access(
    context: types.Context,
    data_dict: dict[str, Any],
) -> types.AuthResult:
    return {"success": False}


def federated_index_profile_refresh(
    context: types.Context,
    data_dict: dict[str, Any],
) -> types.AuthResult:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_list(
    context: types.Context,
    data_dict: dict[str, Any],
) -> types.AuthResult:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_index(
    context: types.Context,
    data_dict: dict[str, Any],
) -> types.AuthResult:
    return authz.is_authorized("federated_index_access", context, data_dict)


def federated_index_profile_clear(
    context: types.Context,
    data_dict: dict[str, Any],
) -> types.AuthResult:
    return authz.is_authorized("federated_index_access", context, data_dict)
