from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_contele_form_answers"
down_revision = None  # coloque aqui o id da sua migration anterior, se existir
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        "contele_form_answers",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("form_id", sa.Text, nullable=True),
        sa.Column("user_id", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source", sa.Text, nullable=False, server_default=sa.text("'contele_checklist'")),
        sa.Column("run_ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"))
    )
    op.create_index("ix_cfa_created_at", "contele_form_answers", ["created_at"])
    op.create_index("ix_cfa_form_id", "contele_form_answers", ["form_id"])
    op.create_index("ix_cfa_user_id", "contele_form_answers", ["user_id"])
    op.create_index("ix_cfa_payload_gin", "contele_form_answers", [sa.text("payload")], postgresql_using="gin")

def downgrade():
    op.drop_index("ix_cfa_payload_gin", table_name="contele_form_answers")
    op.drop_index("ix_cfa_user_id", table_name="contele_form_answers")
    op.drop_index("ix_cfa_form_id", table_name="contele_form_answers")
    op.drop_index("ix_cfa_created_at", table_name="contele_form_answers")
    op.drop_table("contele_form_answers")
