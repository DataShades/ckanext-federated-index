from __future__ import annotations

from typing import Any

from ckan.plugins import Interface

from . import shared


class IFederatedIndex(Interface):
    def federated_index_before_index(
        self,
        pkg_dict: dict[str, Any],
        profile: shared.Profile,
    ) -> dict[str, Any]:
        return pkg_dict

    def federated_index_mangle_package(
        self,
        pkg_dict: dict[str, Any],
        profile: shared.Profile,
    ) -> dict[str, Any]:
        return pkg_dict
