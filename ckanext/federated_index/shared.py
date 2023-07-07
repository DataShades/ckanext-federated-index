from __future__ import annotations

import dataclasses
import json
import logging
from collections import defaultdict
from typing import Any, Iterable

from ckanapi import RemoteCKAN

import ckan.plugins.toolkit as tk

PROFILE_PREFIX: str = "ckanext.federated_index.profile."

log = logging.getLogger(__name__)


@dataclasses.dataclass
class Profile:
    id: str
    url: str
    api_key: str = ""
    index_profile_field: str = "federated_index_profile"
    extras: dict[str, Any] = dataclasses.field(default_factory=dict)
    timeout: int = 10

    def __post_init__(self):
        if isinstance(self.extras, str):
            self.extras = json.loads(self.extras)

    def get_client(self):
        return RemoteCKAN(self.url, self.api_key)

    def fetch_packages(self) -> Iterable[dict[str, Any]]:
        payload = self.extras.get("search_payload", {})
        payload.setdefault("start", 0)

        client = self.get_client()

        while True:
            log.debug(
                "Fetch packages for profile %s starting from %s",
                self.id,
                payload["start"],
            )

            result: dict[str, Any] = client.call_action(
                "package_search",
                payload,
                requests_kwargs={"timeout": self.timeout},
            )
            yield from result["results"]

            payload["start"] += len(result["results"])

            if result["count"] <= payload["start"]:
                break


def iter_profiles() -> Iterable[Profile]:
    """Iterate through federation profiles."""
    profiles: defaultdict[str, dict[str, Any]] = defaultdict(dict)

    for opt, v in tk.config.items():
        if not opt.startswith(PROFILE_PREFIX):
            continue
        profile, attr = opt[len(PROFILE_PREFIX) :].split(".", 1)
        profiles[profile][attr] = v

    for id_, data in profiles.items():
        yield Profile(id=id_, **data)


def get_profile(profile_id: str) -> Profile | None:
    for profile in iter_profiles():
        if profile.id == profile_id:
            return profile
    return None
