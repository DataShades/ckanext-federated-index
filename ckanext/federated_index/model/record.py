from __future__ import annotations

from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped

import ckan.plugins.toolkit as tk
from ckan import model, types
from ckan.lib.dictization import table_dictize
from sqlalchemy.dialects.postgresql import JSONB


def now():
    return datetime.utcnow()


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
    def get(cls, id: str, profile: str):
        """Search for record"""
        return model.Session.get(cls, (id, profile))

    def dictize(self, context: types.Context):
        """Convert into API compatible dictionary"""
        context.setdefault("model", model)  # type: ignore
        return table_dictize(self, context)

    def touch(self):
        self.refreshed_at = now()
