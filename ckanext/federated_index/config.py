from __future__ import annotations

import ckan.plugins.toolkit as tk


def align_schema() -> bool:
    return tk.config["ckanext.federated_index.align_with_local_schema"]


def redirect_remote() -> bool:
    return tk.config["ckanext.federated_index.redirect_missing_federated_datasets"]


def read_endpoints() -> list[str]:
    return tk.config["ckanext.federated_index.dataset_read_endpoints"]


def profile_field() -> str:
    return tk.config["ckanext.federated_index.index_profile_field"]


def url_field() -> str:
    return tk.config["ckanext.federated_index.index_url_field"]
