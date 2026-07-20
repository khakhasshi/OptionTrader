"""Alembic 环境。Schema migration 的唯一权威（Rust SQLx 只消费已迁移 schema）。

DATABASE_URL 环境变量优先于 alembic.ini 中的占位 URL。
本项目使用纯 SQL 迁移（无 SQLAlchemy 模型自动生成），target_metadata 保持 None。
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if (env_url := os.environ.get("DATABASE_URL")) is not None:
    # 归一化为 psycopg3 驱动
    if env_url.startswith("postgresql://"):
        env_url = env_url.replace("postgresql://", "postgresql+psycopg://", 1)
    config.set_main_option("sqlalchemy.url", env_url)

target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
