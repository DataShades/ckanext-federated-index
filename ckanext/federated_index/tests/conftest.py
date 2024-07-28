from __future__ import annotations

import dataclasses
from typing import Any

import ckanapi
import pytest
from pytest_factoryboy import register

import ckan.plugins as p
from ckan.tests import factories

from ckanext.federated_index import interfaces, shared


class TestAppCKAN(ckanapi.TestAppCKAN):
    def call_action(  # noqa: PLR0913
        self,
        action: str,
        data_dict: Any = None,
        context: Any = None,
        apikey: Any = None,
        files: Any = None,
        requests_kwargs: Any = None,
    ):
        return super().call_action(action, data_dict, context, apikey, files)


class TestFederatedIndexPlugin(p.Plugin):
    p.implements(interfaces.IFederatedIndex, inherit=True)

    def federated_index_mangle_package(
        self,
        pkg_dict: dict[str, Any],
        profile: shared.Profile,
    ) -> dict[str, Any]:
        pkg_dict["name"] = "test-" + pkg_dict["name"]
        pkg_dict["id"] = "test-" + pkg_dict["id"]
        return pkg_dict


@pytest.fixture()
def clean_db(reset_db: Any, migrate_db_for: Any):
    reset_db()
    migrate_db_for("federated_index")


@dataclasses.dataclass
class TestProfile(shared.Profile):
    id: str = "test"
    url: str = "http://test.ckan.net"
    test_app: Any = None

    def get_client(self):
        return TestAppCKAN(self.test_app)


@pytest.fixture()
def profile(app: Any):
    return TestProfile(test_app=app)


@register
class PackageFactory(factories.Dataset):
    pass
