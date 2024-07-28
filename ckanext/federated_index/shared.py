from __future__ import annotations

import dataclasses
import json
import logging
from collections import defaultdict
from typing import Any, Iterable

import requests
from ckanapi import RemoteCKAN

import ckan.plugins.toolkit as tk
from ckan.lib.search.query import solr_literal

PROFILE_PREFIX: str = "ckanext.federated_index.profile."

log = logging.getLogger(__name__)
NUMBER_OF_ATTEMPTS = 5


@dataclasses.dataclass
class Profile:
    id: str
    url: str
    api_key: str = ""
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
                " Chrome/117.0.0.0 Safari/537.36"
            ),
        )

    def fetch_packages(
        self,
        search_payload: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        payload = self.extras.get("search_payload", {}).copy()
        payload.update(search_payload)
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

            log.debug("Processing %s packages", len(result["results"]))
            attempt = 0
            yield from result["results"]

            payload["start"] += len(result["results"])

            if result["count"] <= payload["start"]:
                break

    def check_ids(
        self,
        ids: Iterable[str],
    ) -> Iterable[str]:
        payload = self.extras.get("search_payload", {}).copy()
        payload["start"] = 0
        payload["rows"] = 100

        fq_list = payload.get("fq_list", [])

        client = self.get_client()

        missing: set[str] = set()

        while chunk := [id for (_, id) in zip(range(50), ids)]:
            log.debug("Fetch package IDS for profile %s", self.id)

            try:
                result: dict[str, Any] = client.call_action(
                    "package_search",
                    dict(
                        payload,
                        fq_list=fq_list
                        + ["id:({})".format(" OR ".join(map(solr_literal, chunk)))],
                    ),
                    requests_kwargs={"timeout": self.timeout},
                )
            except requests.RequestException:
                log.exception(
                    "Cannot verify datasets for profile %s: %s",
                    self.id,
                    payload,
                )
                break

            log.debug("Processing %s package IDs", len(result["results"]))
            missing |= set(chunk) - {r["id"] for r in result["results"]}

        return missing


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
