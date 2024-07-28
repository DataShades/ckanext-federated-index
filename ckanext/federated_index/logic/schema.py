from __future__ import annotations

from ckan import types
from ckan.logic.schema import validator_args


@validator_args
def profile_refresh(  # noqa: PLR0913
    not_empty: types.Validator,
    boolean_validator: types.Validator,
    federated_index_profile: types.Validator,
    convert_to_json_if_string: types.Validator,
    dict_only: types.Validator,
    default: types.ValidatorFactory,
) -> types.Schema:
    return {
        "profile": [not_empty, federated_index_profile],
        "reset": [boolean_validator],
        "search_payload": [default("{}"), convert_to_json_if_string, dict_only],
        "since_last_refresh": [boolean_validator],
        "index": [boolean_validator],
        "verify": [default(True), boolean_validator],
    }


@validator_args
def profile_list(
    not_empty: types.Validator,
    federated_index_profile: types.Validator,
    default: types.ValidatorFactory,
    int_validator: types.Validator,
) -> types.Schema:
    return {
        "profile": [not_empty, federated_index_profile],
        "offset": [default(0), int_validator],
        "limit": [default(20), int_validator],
    }


@validator_args
def profile_index(
    not_empty: types.Validator,
    json_list_or_string: types.Validator,
    ignore_missing: types.Validator,
    federated_index_profile: types.Validator,
) -> types.Schema:
    return {
        "profile": [not_empty, federated_index_profile],
        "ids": [ignore_missing, json_list_or_string],
    }


@validator_args
def profile_clear(
    not_empty: types.Validator,
    federated_index_profile: types.Validator,
) -> types.Schema:
    return {
        "profile": [not_empty, federated_index_profile],
    }


@validator_args
def profile_remove(
    not_empty: types.Validator,
    federated_index_profile: types.Validator,
) -> types.Schema:
    return {
        "profile": [not_empty, federated_index_profile],
    }
