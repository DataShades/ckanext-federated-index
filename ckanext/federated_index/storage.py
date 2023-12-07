from __future__ import annotations
import sqlalchemy as sa
from typing import Any, Iterable
from ckan.common import json
from ckan.lib import redis
import ckan.plugins.toolkit as tk
from ckan import model
import abc
import enum
from . import shared
from .model import Record


def get_storage(profile: shared.Profile) -> Storage:
    type = profile.extras.get("storage", {}).get("type", "redis")

    if type == "redis":
        return RedisStorage(profile)

    if type == "db":
        return DbStorage(profile)

    raise TypeError(type)


class Storage(abc.ABC):
    profile: shared.Profile

    def __init__(self, profile: shared.Profile):
        self.profile = profile

    @abc.abstractmethod
    def add(self, id: str, pkg: dict[str, Any]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def count(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def reset(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def scan(
        self, offset: int = 0, limit: int | None = None
    ) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, id: str) -> dict[str, Any] | None:
        raise NotImplementedError


class RedisStorage(Storage):
    conn: redis.Redis[bytes]

    def _key(self):
        site_id = tk.config["ckan.site_id"]
        return f"ckan:{site_id}:federated_index:profile:{self.profile.id}:datasets"

    def __init__(self, profile: shared.Profile):
        super().__init__(profile)
        self.conn = redis.connect_to_redis()

    def add(self, id: str, pkg: dict[str, Any]):
        self.conn.hset(self._key(), id, json.dumps(pkg))

    def count(self):
        return self.conn.hlen(self._key())

    def reset(self):
        self.conn.delete(self._key())

    def scan(
        self, offset: int = 0, limit: int | None = None
    ) -> Iterable[dict[str, Any]]:
        if limit is None:
            limit = self.count()

        for idx, (_id, pkg) in enumerate(self.conn.hscan_iter(self._key())):
            if idx < offset:
                continue

            if limit <= 0:
                break
            limit -= 1

            yield json.loads(pkg)

    def get(self, id: str) -> dict[str, Any] | None:
        if pkg := self.conn.hget(self._key(), id):
            return json.loads(pkg)


class DbStorage(Storage):
    def add(self, id: str, pkg: dict[str, Any]):
        record = Record.get(id, self.profile.id)
        if not record:
            record = Record(id=id, profile_id=self.profile.id)

        record.data = pkg
        record.touch()
        model.Session.add(record)
        model.Session.commit()

    def count(self):
        stmt = sa.select(sa.func.count(Record.id)).where(
            Record.profile_id == self.profile.id
        )

        return model.Session.scalar(stmt)

    def reset(self):
        stmt = sa.delete(Record).where(Record.profile_id == self.profile.id)
        model.Session.execute(stmt)
        model.Session.commit()

    def scan(
        self, offset: int = 0, limit: int | None = None
    ) -> Iterable[dict[str, Any]]:
        if limit is not None and limit < 0:
            limit = limit % max(self.count(), 1)

        stmt = (
            sa.select(Record.data)
            .where(Record.profile_id == self.profile.id)
            .offset(offset)
            .limit(limit)
        )

        for pkg in model.Session.scalars(stmt):
            yield pkg

    def get(self, id: str) -> dict[str, Any] | None:
        if record := Record.get(id, self.profile.id):
            return record.data
