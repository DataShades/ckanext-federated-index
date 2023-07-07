from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.exc import IntegrityError

import ckan.plugins as p
import ckan.plugins.toolkit as tk
from ckan import model, types
from ckan.lib import redis, search
from ckan.logic import validate

from ckanext.federated_index import interfaces, shared

from . import schema

log = logging.getLogger(__name__)


@validate(schema.profile_refresh)
def federated_index_profile_refresh(
    context: types.Context,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    tk.check_access("federated_index_profile_refresh", context, data_dict)

    conn: redis.Redis[bytes] = redis.connect_to_redis()
    key = _cache_key(data_dict["profile"])
    conn.delete(key)

    for pkg in data_dict["profile"].fetch_packages():
        conn.rpush(key, json.dumps(pkg))

    return {
        "key": key,
        "size": conn.llen(key),
    }


def _cache_key(profile: shared.Profile) -> str:
    site_id = tk.config["ckan.site_id"]
    return f"ckan:{site_id}:federated_index:profile:{profile.id}:datasets"


@validate(schema.profile_list)
def federated_index_profile_list(
    context: types.Context,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    tk.check_access("federated_index_profile_list", context, data_dict)

    conn: redis.Redis[bytes] = redis.connect_to_redis()
    key = _cache_key(data_dict["profile"])

    return {
        "results": [
            json.loads(pkg)
            for pkg in conn.lrange(key, data_dict["offset"], data_dict["limit"])
        ],
        "count": conn.llen(key),
    }


@validate(schema.profile_index)
def federated_index_profile_index(
    context: types.Context,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    tk.check_access("federated_index_profile_index", context, data_dict)

    profile: shared.Profile = data_dict["profile"]
    conn: redis.Redis[bytes] = redis.connect_to_redis()
    key = _cache_key(profile)

    package_index: search.PackageSearchIndex = search.index_for(model.Package)

    for idx in range(conn.llen(key)):
        pkg_str = conn.lindex(key, idx)
        if not pkg_str:
            log.warning("Cached package with index %d is missing", idx)
            msg = f"Cached package: {idx}"
            raise tk.ObjectNotFound(msg)

        pkg_dict = json.loads(pkg_str)

        if model.Package.get(pkg_dict["name"]):
            log.warning("Package with name %s already exists", pkg_dict["name"])
            continue

        # hack: create a dataset object to force ckan setting
        # proper permission labels
        model.Session.add(
            model.Package(
                id=pkg_dict["id"],
                state=model.State.ACTIVE,
                private=False,
                name=pkg_dict["name"],
            ),
        )

        try:
            model.Session.flush()
        except IntegrityError:
            log.exception("Cannot index package %s", pkg_dict["name"])
            model.Session.rollback()
            continue

        for plugin in p.PluginImplementations(interfaces.IFederatedIndex):
            pkg_dict = plugin.federated_index_before_index(pkg_dict, profile)

        try:
            package_index.remove_dict(pkg_dict)
            package_index.update_dict(pkg_dict, True)
        except (search.SearchIndexError, TypeError):
            log.exception("Cannot index package %s", pkg_dict["name"])
        else:
            log.debug("Successfully indexed package %s", pkg_dict["name"])
        finally:
            model.Session.rollback()

    package_index.commit()

    return {
        "key": key,
        "size": conn.llen(key),
    }


@validate(schema.profile_clear)
def federated_index_profile_clear(
    context: types.Context,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    tk.check_access("federated_index_profile_clear", context, data_dict)
    profile: shared.Profile = data_dict["profile"]

    conn = search.make_connection()
    query = f"+{profile.index_profile_field}:{profile.id}"

    conn.delete(q=query)

    conn.commit()

    return {}
