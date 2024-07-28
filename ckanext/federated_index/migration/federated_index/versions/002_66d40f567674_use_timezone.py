"""Use timezone.

Revision ID: 66d40f567674
Revises: fce2130b4dc0
Create Date: 2024-07-26 15:19:53.189295

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "66d40f567674"
down_revision = "fce2130b4dc0"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "federated_index_package", "refreshed_at", type_=sa.DateTime(timezone=True)
    )


def downgrade():
    op.alter_column(
        "federated_index_package", "refreshed_at", type_=sa.DateTime(timezone=False)
    )
