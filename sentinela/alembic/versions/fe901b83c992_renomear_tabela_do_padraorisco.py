"""Renomear tabela do PadraoRisco

Revision ID: fe901b83c992
Revises: 66d9f966e983
Create Date: 2018-02-20 13:18:21.154690

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fe901b83c992'
down_revision = '66d9f966e983'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('padroesrisco',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('nome', sa.String(length=20), nullable=True),
    sa.Column('base_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['base_id'], ['basesorigem.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('nome')
    )
    """
    op.drop_table('bases')
    with op.batch_alter_table('parametrosrisco', schema=None) as batch_op:
        batch_op.drop_constraint('padrarisco_parametros', type_='foreignkey')
        batch_op.create_foreign_key(None, 'padroresrisco', ['padraorisco_id'], ['id'])

    """# ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    """with op.batch_alter_table('parametrosrisco', schema=None) as batch_op:
        batch_op.drop_constraint(None, type_='foreignkey')
        batch_op.create_foreign_key('padrarisco_parametros', 'bases', ['padraorisco_id'], ['id'])

    op.create_table('bases',
    sa.Column('id', sa.INTEGER(), nullable=False),
    sa.Column('nome', sa.VARCHAR(length=20), nullable=True),
    sa.Column('base_id', sa.INTEGER(), nullable=True),
    sa.ForeignKeyConstraint(['base_id'], ['basesorigem.id'], name='baseorigem_base'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('nome')
    )
    op.drop_table('padroresrisco')
    """# ### end Alembic commands ###