from __future__ import annotations

import abc
import os
import pathlib
from typing import Any, Iterable

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

import ckan.plugins.toolkit as tk
from ckan import model
from ckan.common import json
from ckan.lib import redis

from . import shared
from .model import Record


def get_storage(profile: shared.Profile) -> Storage:
    type = profile.extras.get("storage", {}).get("type", "db")

    if type == "redis":
        return RedisStorage(profile)

    if type == "db":
        return DbStorage(profile)

    if type == "sqlite":
        return SqliteStorage(profile)

    if type == "fs":
        return FsStorage(profile)

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
    def remove(self, id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def reset(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def scan(
        self,
        offset: int = 0,
        limit: int | None = None,
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

    def remove(self, id: str):
        self.conn.hdel(self._key(), id)

    def reset(self):
        self.conn.delete(self._key())

    def scan(
        self,
        offset: int = 0,
        limit: int | None = None,
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
    def __init__(self, profile: shared.Profile):
        super().__init__(profile)
        self.session = model.Session

    def add(self, id: str, pkg: dict[str, Any]):
        record: Record | None = self.session.scalar(Record.select(self.profile.id, id))

        if not record:
            record = Record(id=id, profile_id=self.profile.id)

        record.data = pkg
        record.touch()
        self.session.add(record)
        self.session.commit()

    def count(self) -> int:
        stmt = sa.select(sa.func.count(Record.id)).where(
            Record.profile_id == self.profile.id,
        )

        return self.session.scalar(stmt)

    def remove(self, id: str):
        stmt = (
            sa.delete(Record)
            .where(Record.profile_id == self.profile.id)
            .where(Record.id == id)
        )
        self.session.execute(stmt)
        self.session.commit()

    def reset(self):
        stmt = sa.delete(Record).where(Record.profile_id == self.profile.id)
        self.session.execute(stmt)
        self.session.commit()

    def scan(
        self,
        offset: int = 0,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        if limit is not None and limit < 0:
            limit = limit % max(self.count(), 1)

        stmt = (
            sa.select(Record.data)
            .where(Record.profile_id == self.profile.id)
            .offset(offset)
            .limit(limit)
        )

        yield from self.session.scalars(stmt)

    def get(self, id: str) -> dict[str, Any] | None:
        if record := self.session.scalar(Record.select(self.profile.id, id)):
            return record.data


class SqliteStorage(Storage):
    table = "federated_index_package"

    def __init__(self, profile: shared.Profile):
        super().__init__(profile)

        url = profile.extras.setdefault("storage", {}).get("url")
        if not url:
            path = os.path.join(tk.config["ckan.storage_path"], "federated_index")
            if not os.path.isdir(path):
                os.mkdir(path)

            url = "sqlite:///" + os.path.join(
                path,
                f"{profile.id}.sqlite3.db",
            )

        engine = sa.create_engine(url)
        if not engine.has_table(self.table):
            engine.execute(
                sa.text(
                    f"""
                CREATE TABLE IF NOT EXISTS {sa.table(self.table)}(
                id TEXT,
                profile_id TEXT,
                refreshed_at TEXT,
                data TEXT,
                PRIMARY KEY(id, profile_id)
                )
                """,
                ),
            )
        self.session = sessionmaker(engine)()

    def add(self, id: str, pkg: dict[str, Any]):
        record = self.session.scalar(Record.select(self.profile.id, id))
        if not record:
            record = Record(id=id, profile_id=self.profile.id)

        record.data = pkg
        record.touch()
        self.session.add(record)
        self.session.commit()

    def count(self) -> int:
        stmt = sa.select(sa.func.count()).select_from(Record.select(self.profile.id))

        return self.session.scalar(stmt)

    def remove(self, id: str):
        stmt = (
            sa.delete(Record)
            .where(Record.profile_id == self.profile.id)
            .where(Record.id == id)
        )
        self.session.execute(stmt)
        self.session.commit()

    def reset(self):
        stmt = sa.delete(Record).where(Record.profile_id == self.profile.id)
        self.session.execute(stmt)
        self.session.commit()

    def scan(
        self,
        offset: int = 0,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        if limit is not None and limit < 0:
            limit = limit % max(self.count(), 1)

        stmt = (
            sa.select(Record.data)
            .where(Record.profile_id == self.profile.id)
            .offset(offset)
            .limit(limit)
        )

        yield from self.session.scalars(stmt)

    def get(self, id: str) -> dict[str, Any] | None:
        if record := self.session.scalar(Record.select(self.profile.id, id)):
            return record.data


class FsStorage(Storage):
    def __init__(self, profile: shared.Profile):
        super().__init__(profile)

        path = profile.extras.setdefault("storage", {}).get("path")
        if not path:
            path = os.path.join(
                tk.config["ckan.storage_path"],
                "federated_index",
                profile.id,
            )

        if not os.path.isdir(path):
            os.makedirs(path)

        self.path = pathlib.Path(path)

    def add(self, id: str, pkg: dict[str, Any]):
        filepath = self.path / f"{id}.json"
        with filepath.open("w") as dest:
            json.dump(pkg, dest)

    def count(self):
        return len(list(self.path.iterdir()))

    def remove(self, id: str):
        filepath = self.path / f"{id}.json"
        filepath.unlink()

    def reset(self):
        for filepath in self.path.iterdir():
            filepath.unlink()

    def scan(
        self,
        offset: int = 0,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        if limit is not None and limit < 0:
            limit = limit % max(self.count(), 1)

        for filepath in self.path.iterdir():
            yield json.load(filepath.open())

    def get(self, id: str) -> dict[str, Any] | None:
        filepath = self.path / f"{id}.json"
        if filepath.exists():
            return json.load(filepath.open())
