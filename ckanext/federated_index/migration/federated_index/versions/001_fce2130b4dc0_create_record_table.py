"""create record table.

Revision ID: fce2130b4dc0
Revises:
Create Date: 2023-12-07 18:17:14.393496

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "fce2130b4dc0"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "federated_index_package",
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
        sa.Column(
            "refreshed_at",
            sa.DateTime,
            server_default=sa.func.current_timestamp(),
        ),
        sa.Column("data", JSONB, nullable=False),
    )


def downgrade():
    op.drop_table(
        "federated_index_package",
    )
