from ckan.logic.schema import validator_args


@validator_args
def profile_refresh(
    not_empty,
    federated_index_profile,
):
    return {
        "profile": [not_empty, federated_index_profile],
    }


@validator_args
def profile_list(
    not_empty,
    federated_index_profile,
    default,
    int_validator,
):
    return {
        "profile": [not_empty, federated_index_profile],
        "offset": [default(0), int_validator],
        "limit": [default(-1), int_validator],
    }


@validator_args
def profile_index(
    not_empty,
    federated_index_profile,
):
    return {
        "profile": [not_empty, federated_index_profile],
    }


@validator_args
def profile_clear(
    not_empty,
    federated_index_profile,
):
    return {
        "profile": [not_empty, federated_index_profile],
    }
