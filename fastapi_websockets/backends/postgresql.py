from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import uuid4

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed, InvalidChannelLayerConfig
from fastapi_websockets.serialization import JsonSerializer


class PostgreSQLChannelLayer(BaseChannelLayer):
    """PostgreSQL-backed layer using tables for storage and NOTIFY for signaling."""

    def __init__(
        self,
        dsn: str = "postgresql://postgres:postgres@localhost:5432/postgres",
        schema: str = "fastapi_websockets",
        channel_expiry: int = 60,
        group_expiry: int = 86400,
        poll_interval: float = 0.1,
        ensure_schema: bool = True,
        pool: Any | None = None,
        serializer: Any | None = None,
        **config: Any,
    ) -> None:
        super().__init__(
            dsn=dsn,
            schema=schema,
            channel_expiry=channel_expiry,
            group_expiry=group_expiry,
            poll_interval=poll_interval,
            ensure_schema=ensure_schema,
            **config,
        )
        self.dsn = dsn
        self.schema = schema
        self.channel_expiry = channel_expiry
        self.group_expiry = group_expiry
        self.poll_interval = poll_interval
        self.ensure_schema = ensure_schema
        self.serializer = serializer or JsonSerializer()
        self._pool = pool
        self._owns_pool = pool is None
        self._closed = False
        self._schema_ready = pool is not None or not ensure_schema

    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("channel", channel)
        await self._get_pool()
        expires_at = self._expires_at(self.channel_expiry)
        payload = self.serializer.dumps(message).decode("utf-8")
        await self._execute(
            f"""
            INSERT INTO {self._qualify("messages")} (channel, payload, expires_at)
            VALUES ($1, $2::jsonb, $3)
            """,
            channel,
            payload,
            expires_at,
        )
        await self._execute("SELECT pg_notify($1, $2)", self._notify_channel(channel), channel)

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        self._ensure_open()
        self._validate_name("channel", channel)
        await self._get_pool()
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout

        while True:
            row = await self._fetchrow(
                f"""
                WITH next_message AS (
                    SELECT id, payload
                    FROM {self._qualify("messages")}
                    WHERE channel = $1
                      AND (expires_at IS NULL OR expires_at > NOW())
                    ORDER BY id
                    LIMIT 1
                )
                DELETE FROM {self._qualify("messages")}
                WHERE id IN (SELECT id FROM next_message)
                RETURNING payload
                """,
                channel,
            )
            if row is not None:
                payload = row["payload"]
                return self.serializer.loads(payload)

            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Timed out waiting for channel '{channel}'")

            sleep_for = self.poll_interval
            if deadline is not None:
                sleep_for = min(sleep_for, max(deadline - asyncio.get_running_loop().time(), 0))
                if sleep_for == 0:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'")
            await asyncio.sleep(sleep_for)

    async def new_channel(self, prefix: str = "specific") -> str:
        self._ensure_open()
        self._validate_name("channel", prefix)
        return f"{prefix}.{uuid4().hex}"

    async def group_add(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        await self._get_pool()
        expires_at = self._expires_at(self.group_expiry)
        await self._execute(
            f"""
            INSERT INTO {self._qualify("group_members")} (group_name, channel, expires_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (group_name, channel)
            DO UPDATE SET expires_at = EXCLUDED.expires_at
            """,
            group,
            channel,
            expires_at,
        )

    async def group_discard(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        await self._get_pool()
        await self._execute(
            f"DELETE FROM {self._qualify('group_members')} WHERE group_name = $1 AND channel = $2",
            group,
            channel,
        )

    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        await self._get_pool()
        rows = await self._fetch(
            f"""
            SELECT channel
            FROM {self._qualify("group_members")}
            WHERE group_name = $1
              AND (expires_at IS NULL OR expires_at > NOW())
            """,
            group,
        )
        for row in rows:
            await self.send(row["channel"], message)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pool is not None and self._owns_pool:
            close = getattr(self._pool, "close", None) or getattr(self._pool, "aclose", None)
            if close is not None:
                result = close()
                if result is not None:
                    await result

    async def _get_pool(self) -> Any:
        if self._pool is None:
            try:
                import asyncpg
            except ImportError as exc:
                raise InvalidChannelLayerConfig(
                    "PostgreSQL backend requires the optional dependency group: pip install 'fastapi-websockets[postgresql]'"
                ) from exc
            self._pool = await asyncpg.create_pool(self.dsn)

        if not self._schema_ready and self.ensure_schema:
            await self._ensure_schema()
            self._schema_ready = True
        return self._pool

    async def _ensure_schema(self) -> None:
        pool = self._pool
        if pool is None:
            raise InvalidChannelLayerConfig("PostgreSQL pool is not initialized")
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"""
            CREATE TABLE IF NOT EXISTS {self._qualify("messages")} (
                id BIGSERIAL PRIMARY KEY,
                channel TEXT NOT NULL,
                payload JSONB NOT NULL,
                expires_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.schema}_messages_channel_id_idx
            ON {self._qualify("messages")} (channel, id)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self._qualify("group_members")} (
                group_name TEXT NOT NULL,
                channel TEXT NOT NULL,
                expires_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (group_name, channel)
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.schema}_group_members_group_idx
            ON {self._qualify("group_members")} (group_name)
            """,
        ]
        for statement in statements:
            acquire = getattr(pool, "acquire", None)
            if acquire is None:
                await pool.execute(statement)
                continue
            async with pool.acquire() as connection:
                await connection.execute(statement)

    async def _execute(self, query: str, *args: Any) -> Any:
        return await self._run_connection_method("execute", query, *args)

    async def _fetch(self, query: str, *args: Any) -> Any:
        return await self._run_connection_method("fetch", query, *args)

    async def _fetchrow(self, query: str, *args: Any) -> Any:
        return await self._run_connection_method("fetchrow", query, *args)

    async def _run_connection_method(self, method: str, query: str, *args: Any) -> Any:
        pool = await self._get_pool()
        acquire = getattr(pool, "acquire", None)
        if acquire is None:
            fn = getattr(pool, method)
            return await fn(query, *args)

        async with pool.acquire() as connection:
            fn = getattr(connection, method)
            return await fn(query, *args)

    def _qualify(self, table: str) -> str:
        return f"{self.schema}.{table}"

    def _notify_channel(self, channel: str) -> str:
        return f"{self.schema}_{channel.replace('.', '_')}"

    def _expires_at(self, seconds: int) -> datetime | None:
        if seconds <= 0:
            return None
        return datetime.now(timezone.utc) + timedelta(seconds=seconds)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ChannelLayerClosed("Channel layer has been closed")

    @staticmethod
    def _validate_name(kind: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{kind.title()} name must be a non-empty string")
