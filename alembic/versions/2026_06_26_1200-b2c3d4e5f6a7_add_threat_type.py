"""Add nullable threat_type column to threat_model

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-26 12:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Explicit organism type for the agstack-pnd adapter
    # (fungus|bacterium|oomycete|insect|mite). Nullable: existing rows stay NULL
    # and the adapter keeps inferring the type from fuzzy_rules.
    op.add_column(
        "threat_model",
        sa.Column("threat_type", sa.String(length=20), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("threat_model", "threat_type")
