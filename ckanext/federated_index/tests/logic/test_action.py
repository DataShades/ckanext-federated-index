from __future__ import annotations

from typing import Any

import pytest

from ckan.tests.helpers import call_action

from ckanext.federated_index import shared


@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
class TestRefresh:
    def test_verification(self, profile: shared.Profile, package_factory: Any):
        """Removed packages dropped from storage."""

        pkgs = package_factory.create_batch(3)

        result = call_action("federated_index_profile_refresh", profile=profile)
        assert result == {"count": 3, "profile": profile.id}

        call_action("package_delete", id=pkgs[-1]["id"])

        result = call_action(
            "federated_index_profile_refresh", profile=profile, verify=False
        )
        assert result["count"] == 3

        result = call_action("federated_index_profile_refresh", profile=profile)
        assert result["count"] == 2
