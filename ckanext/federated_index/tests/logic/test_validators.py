from __future__ import annotations

import pytest
from faker import Faker

import ckan.plugins.toolkit as tk

from ckanext.federated_index import shared
from ckanext.federated_index.logic import validators


class TestFederatedIndexProfile:
    def test_missing(self, faker: Faker):
        """Missing profile produces validation error."""
        with pytest.raises(tk.Invalid):
            validators.federated_index_profile(faker.word())

    @pytest.mark.ckan_config(f"{shared.PROFILE_PREFIX}test.url", "http://example.com")
    def test_existing(self, faker: Faker):
        """Validator return a Profile when correct ID is passed.

        If validator receives Profile, it's returned unchanged.
        """
        profile = validators.federated_index_profile("test")
        assert isinstance(profile, shared.Profile)
        assert validators.federated_index_profile(profile) == profile
