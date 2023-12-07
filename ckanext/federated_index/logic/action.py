from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

from sqlalchemy.exc import IntegrityError

import ckan.plugins as p
import ckan.plugins.toolkit as tk
from ckan import model
from ckan.lib import search
from ckan.logic import validate

from ckanext.federated_index import interfaces, shared, config, storage

from . import schema

log = logging.getLogger(__name__)


@validate(schema.profile_refresh)
def federated_index_profile_refresh(
    context: Any,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    """Pull data from the remote file specified by profile settings.

    Args:
        profile(str|Profile): name of the profile or Profile instance
        reset(bool): remove existing data
        search_payload(dict[str, Any]): search parameters
        since_last_refresh(bool): only fetch packages updated since last refresh

    """

    tk.check_access("federated_index_profile_refresh", context, data_dict)
    profile: shared.Profile = data_dict["profile"]
    payload = data_dict["search_payload"]

    db = storage.get_storage(profile)

    if data_dict["since_last_refresh"]:
        conn = search.make_connection()
        query = f"+{config.profile_field()}:{profile.id}"
        resp = conn.search(query, sort="metadata_modified desc", rows=1)
        if resp.docs:
            since: datetime = resp.docs[0]["metadata_modified"]
            payload.setdefault("fq_list", []).append(
                f"metadata_modified:[{since.isoformat()}Z TO *]"
            )

    if data_dict["reset"]:
        db.reset()

    for pkg in profile.fetch_packages(payload):
        db.add(pkg["id"], pkg)

    return {
        "profile": profile.id,
        "count": db.count(),
    }


@validate(schema.profile_list)
def federated_index_profile_list(
    context: Any,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    """List stored datasets from the federation profile.

    Args:
        profile(str|Profile): name of the profile or Profile instance
        offset(int, optional): skip N records
        limit(int, default: 20): show N records at most
    """

    tk.check_access("federated_index_profile_list", context, data_dict)

    db = storage.get_storage(data_dict["profile"])

    return {
        "results": list(db.scan(offset=data_dict["offset"], limit=data_dict["limit"])),
        "count": db.count(),
    }


@validate(schema.profile_index)
def federated_index_profile_index(
    context: Any,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    """Index stored data for the profile.

    Args:
        profile(str|Profile): name of the profile or Profile instance

    """
    tk.check_access("federated_index_profile_index", context, data_dict)

    profile: shared.Profile = data_dict["profile"]
    db = storage.get_storage(data_dict["profile"])
    package_index: search.PackageSearchIndex = search.index_for(model.Package)

    if ids := data_dict.get("ids"):
        packages = filter(None, map(db.get, ids))
    else:
        packages = db.scan()

    for pkg_dict in packages:
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
        "profile": data_dict["profile"].id,
        "count": db.count(),
    }


@validate(schema.profile_clear)
def federated_index_profile_clear(
    context: Any,
    data_dict: dict[str, Any],
) -> dict[str, Any]:
    """Remove from search index all datasets of the given profile.

    Args:
        profile(str|Profile): name of the profile or Profile instance

    """
    tk.check_access("federated_index_profile_clear", context, data_dict)
    profile: shared.Profile = data_dict["profile"]

    conn = search.make_connection()
    query = f"+{config.profile_field()}:{profile.id}"

    resp = conn.search(q=query, rows=0)

    conn.delete(q=query)

    conn.commit()

    return {
        "profile": data_dict["profile"].id,
        "count": resp.hits,
    }
