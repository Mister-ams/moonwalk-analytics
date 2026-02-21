"""Alembic environment configuration for Moonwalk Analytics (analytics schema)."""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from config import ANALYTICS_DATABASE_URL

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", ANALYTICS_DATABASE_URL)

# Migrations are written manually — no autogenerate.
target_metadata = None


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a live DB connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        version_table_schema="analytics",
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Bootstrap pass: AUTOCOMMIT so CREATE SCHEMA/EXTENSION commit immediately.
    # This must happen before Alembic opens its migration transaction, because
    # Alembic writes alembic_version to analytics.alembic_version before upgrade() runs.
    with connectable.connect().execution_options(isolation_level="AUTOCOMMIT") as boot:
        boot.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        boot.execute(text("CREATE SCHEMA IF NOT EXISTS hr"))
        boot.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))

    # Migration pass: normal transactional DDL.
    # Do NOT execute any statements before context.configure() — any query on
    # the connection triggers SQLAlchemy autobegin, which makes Alembic set
    # _in_external_transaction=True and return nullcontext() from
    # begin_transaction(), skipping the commit entirely.
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            version_table_schema="analytics",
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
