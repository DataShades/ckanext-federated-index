from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import ckan.plugins as p
import ckan.plugins.toolkit as tk
from ckan.lib.search.query import solr_literal

from . import interfaces, shared
from .logic import action, auth, validators

if TYPE_CHECKING:
    from ckan.common import CKANConfig

PKG_MANDATORY_FIELDS: list[str] = [
    "id",
    "extras",
    "metadata_created",
    "metadata_modified",
    "spatial",
    "num_resources",
    "resources",
    "num_tags",
    "tags",
    "state",
    "private",
    "type",
]
RES_MANDATORY_FIELDS: list[str] = [
    "id",
    "created",
    "metadata_modified",
]

NOT_FOUND = 404


# @tk.blanket.config_declarations
class FederatedIndexPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IMiddleware, inherit=True)
    p.implements(interfaces.IFederatedIndex, inherit=True)

    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IValidators)

    get_validators = staticmethod(validators.get_validators)
    get_actions = staticmethod(action.get_actions)
    get_auth_functions = staticmethod(auth.get_auth_functions)

    def make_middleware(self, app, config):
        app.after_request(_redirect_missing_federated_packages)
        return app

    def update_config(self, config: CKANConfig) -> None:
        tk.add_template_directory(config, "templates")

    def federated_index_before_index(
        self,
        pkg_dict: dict[str, Any],
        profile: shared.Profile,
    ) -> dict[str, Any]:
        pkg_dict[profile.index_profile_field] = profile.id
        pkg_dict.setdefault("extras", []).extend(
            [
                {
                    "key": profile.index_profile_field,
                    "value": profile.id,
                },
                {
                    "key": "federated_index_remote_url",
                    "value": urljoin(
                        profile.url.rstrip("/") + "/",
                        f"{pkg_dict['type']}/{pkg_dict['id']}",
                    ),
                },
            ],
        )

        if tk.asbool(tk.config.get("ckanext.federated_index.align_with_local_schema")):
            _align_with_local_schema(pkg_dict)

        return pkg_dict


def _redirect_missing_federated_packages(response: Any):
    if response.status_code != NOT_FOUND or not tk.asbool(
        tk.config.get("ckanext.federated_index.redirect_missing_federated_datasets"),
    ):
        return response

    dataset_endpoints = tk.aslist(
        tk.config.get("ckanext.federated_index.dataset_read_endpoints", "dataset.read"),
    )

    if ".".join(tk.get_endpoint()) not in dataset_endpoints:
        return response

    view_args = tk.request.view_args or {}
    pkg_id = view_args.get("id")
    if not pkg_id:
        return response

    pkg_id = solr_literal(pkg_id)

    fq = f"((id:{pkg_id} OR name:{pkg_id}) AND extras_federated_index_remote_url:*)"
    result = tk.get_action("package_search")(
        {},
        {
            "fq": fq,
            "fl": ",".join(
                [
                    "extras_federated_index_profile",
                    "name",
                    "extras_federated_index_remote_url",
                ],
            ),
            "rows": 1,
        },
    )["results"]

    if result and (url := result[0].get("federated_index_remote_url")):
        return tk.redirect_to(url)

    return response


def _align_with_local_schema(dataset: dict[str, Any]) -> None:
    """Throw away fields that doesn't exist in our local dataset schema"""

    schema: dict[str, Any] = tk.h.scheming_get_schema("dataset", dataset["type"])

    if not schema:
        return

    dataset_exclude_fields: list[str] = _get_dataset_fields_for_exclusion(
        dataset,
        [field["field_name"] for field in schema["dataset_fields"]],
    )
    resource_exclude_fields: list[str] = _get_resource_fields_for_exclusion(
        dataset.get("resources", []),
        [field["field_name"] for field in schema["resource_fields"]],
    )

    _exclude_fields(dataset, dataset_exclude_fields)

    for resource in dataset.get("resources", []):
        _exclude_fields(resource, resource_exclude_fields)


def _get_dataset_fields_for_exclusion(
    dataset: dict[str, Any],
    dataset_fields: list[dict[str, Any]],
) -> list[str]:
    exclude_fields: list[str] = []

    for field_name in dataset:
        if field_name in dataset_fields:
            continue

        if field_name in PKG_MANDATORY_FIELDS:
            continue

        exclude_fields.append(field_name)

    return exclude_fields


def _get_resource_fields_for_exclusion(
    resources: list[dict[str, Any]],
    resource_fields: list[dict[str, Any]],
) -> list[str]:
    exclude_fields: list[str] = []

    for resource in resources:
        for resource_field in resource:
            if resource_field in resource_fields:
                continue

            if resource_field in RES_MANDATORY_FIELDS:
                continue

            exclude_fields.append(resource_field)

    return exclude_fields


def _exclude_fields(entity_dict: dict[str, Any], exclude_fields: list[str]) -> None:
    for field_name in exclude_fields:
        entity_dict.pop(field_name, None)
