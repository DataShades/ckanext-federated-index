from __future__ import annotations

import dataclasses
import json
import logging
from collections import defaultdict
from typing import Any, Iterable

import requests
from ckanapi import RemoteCKAN

import ckan.plugins.toolkit as tk

PROFILE_PREFIX: str = "ckanext.federated_index.profile."

log = logging.getLogger(__name__)
NUMBER_OF_ATTEMPTS = 5


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
        return RemoteCKAN(
            self.url,
            self.api_key,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/117.0.0.0 Safari/537.36",
            ),
        )

    def fetch_packages(self) -> Iterable[dict[str, Any]]:
        payload = self.extras.get("search_payload", {})
        payload.setdefault("start", 0)

        client = self.get_client()

        attempt = 0

        while True:
            log.debug(
                "Fetch packages for profile %s starting from %s",
                self.id,
                payload["start"],
            )

            try:
                result: dict[str, Any] = client.call_action(
                    "package_search",
                    payload,
                    requests_kwargs={"timeout": self.timeout},
                )
            except requests.RequestException:
                log.exception(
                    "Cannot pull datasets for profile %s: %s",
                    self.id,
                    payload,
                )
                attempt += 1

                if attempt > NUMBER_OF_ATTEMPTS:
                    break

                continue

            attempt = 0
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
