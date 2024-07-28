from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped

import ckan.plugins.toolkit as tk
from ckan import model, types
from ckan.lib.dictization import table_dictize


def now():
    return datetime.now(timezone.utc)


class Record(tk.BaseModel):  # type: ignore
    __table__ = sa.Table(
        "federated_index_package",
        tk.BaseModel.metadata,
        sa.Column(
            "id",
            sa.UnicodeText,
            primary_key=True,
        ),
        sa.Column(
            "profile_id",
            sa.UnicodeText,
            primary_key=True,
        ),
        sa.Column("refreshed_at", sa.DateTime, default=now),
        sa.Column("data", JSONB, nullable=False),
    )

    id: Mapped[int]
    profile_id: Mapped[str]
    refreshed_at: Mapped[datetime]
    data: Mapped[dict[str, Any]]

    @classmethod
    def select(cls, profile: str, id: str | None = None):
        """Select the whole profile or single record."""
        stmt = sa.select(cls).where(cls.profile_id == profile)

        if id:
            stmt = stmt.where(cls.id == id)

        return stmt

    def dictize(self, context: types.Context):
        """Convert into API compatible dictionary."""
        context.setdefault("model", model)  # type: ignore
        return table_dictize(self, context)

    def touch(self):
        self.refreshed_at = now()
