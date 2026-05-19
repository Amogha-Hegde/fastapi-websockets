import asyncio
from datetime import datetime, timezone

from fastapi_websockets.backends.postgresql import PostgreSQLChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed


class FakePostgresPool:
    def __init__(self) -> None:
        self.messages = []
        self.group_members = {}
        self.closed = False
        self.executed = []
        self.listener_connections = []

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO fastapi_websockets.messages"):
            channel, payload, expires_at = args
            self.messages.append(
                {
                    "id": len(self.messages) + 1,
                    "channel": channel,
                    "payload": payload,
                    "expires_at": expires_at,
                }
            )
            return "INSERT 0 1"
        if normalized.startswith("SELECT pg_notify"):
            return "SELECT 1"
        if "INSERT INTO fastapi_websockets.group_members" in normalized:
            group, channel, expires_at = args
            self.group_members[(group, channel)] = expires_at
            return "INSERT 0 1"
        if normalized == "DELETE FROM fastapi_websockets.messages WHERE expires_at IS NOT NULL AND expires_at <= NOW()":
            now = datetime.now(timezone.utc)
            self.messages = [
                message
                for message in self.messages
                if message["expires_at"] is None or message["expires_at"] > now
            ]
            return "DELETE"
        if normalized == "DELETE FROM fastapi_websockets.group_members WHERE expires_at IS NOT NULL AND expires_at <= NOW()":
            now = datetime.now(timezone.utc)
            self.group_members = {
                key: expires_at
                for key, expires_at in self.group_members.items()
                if expires_at is None or expires_at > now
            }
            return "DELETE"
        if normalized.startswith("DELETE FROM fastapi_websockets.group_members"):
            group, channel = args
            self.group_members.pop((group, channel), None)
            return "DELETE 1"
        if normalized.startswith("CREATE ") or normalized.startswith("CREATE SCHEMA"):
            return "CREATE"
        raise AssertionError(f"Unexpected execute query: {query}")

    async def fetchrow(self, query: str, *args):
        channel = args[0]
        now = datetime.now(timezone.utc)
        for index, message in enumerate(self.messages):
            if message["channel"] != channel:
                continue
            expires_at = message["expires_at"]
            if expires_at is not None and expires_at <= now:
                continue
            self.messages.pop(index)
            return {"payload": message["payload"]}
        return None

    async def fetch(self, query: str, *args):
        group = args[0]
        now = datetime.now(timezone.utc)
        rows = []
        for (group_name, channel), expires_at in self.group_members.items():
            if group_name != group:
                continue
            if expires_at is not None and expires_at <= now:
                continue
            rows.append({"channel": channel})
        return rows

    async def close(self):
        self.closed = True

    def acquire(self):
        return FakePostgresAcquire(self)


class FakePostgresAcquire:
    def __init__(self, pool: FakePostgresPool) -> None:
        self.pool = pool
        self.connection = FakePostgresConnection(pool)

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


class FakePostgresConnection:
    def __init__(self, pool: FakePostgresPool) -> None:
        self.pool = pool
        self.listeners = {}
        self.pool.listener_connections.append(self)

    async def execute(self, query: str, *args):
        return await self.pool.execute(query, *args)

    async def fetch(self, query: str, *args):
        return await self.pool.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        return await self.pool.fetchrow(query, *args)

    async def add_listener(self, channel: str, callback) -> None:
        self.listeners.setdefault(channel, []).append(callback)

    async def remove_listener(self, channel: str, callback) -> None:
        callbacks = self.listeners.get(channel, [])
        if callback in callbacks:
            callbacks.remove(callback)

    def notify(self, channel: str, payload: str = "") -> None:
        for callback in self.listeners.get(channel, []):
            callback(self, 0, channel, payload)


def test_send_and_receive_round_trip() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await layer.receive("chat.room", timeout=0.05)
        assert message == {"type": "message", "text": "hello"}

    asyncio.run(run())


def test_send_and_receive_round_trip_with_bytes() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False)
        payload = {
            "type": "websocket.send",
            "mode": "bytes",
            "body": b"\x00\x01hello",
        }
        await layer.send("chat.room", payload)
        message = await layer.receive("chat.room", timeout=0.05)
        assert message == payload

    asyncio.run(run())


def test_group_send_fans_out_to_members() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False)
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")
        await layer.group_send("room", {"type": "broadcast", "text": "hi"})
        first = await layer.receive("channel.one", timeout=0.05)
        second = await layer.receive("channel.two", timeout=0.05)
        assert first["text"] == "hi"
        assert second["text"] == "hi"

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False)
        await layer.group_add("room", "channel.one")
        await layer.group_discard("room", "channel.one")
        await layer.group_send("room", {"type": "broadcast"})
        try:
            await layer.receive("channel.one", timeout=0.02)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        else:
            raise AssertionError("Expected timeout after group_discard")

    asyncio.run(run())


def test_close_rejects_new_operations() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False)
        await layer.close()
        assert pool.closed is False
        try:
            await layer.send("chat.room", {"type": "message"})
        except ChannelLayerClosed:
            pass
        else:
            raise AssertionError("Expected ChannelLayerClosed after close")

    asyncio.run(run())


def test_close_closes_internal_pool() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer()
        layer._pool = pool
        layer._owns_pool = True
        layer._schema_ready = True
        await layer.close()
        assert pool.closed is True

    asyncio.run(run())


def test_notify_channel_is_bounded_for_long_channel_names() -> None:
    layer = PostgreSQLChannelLayer(ensure_schema=False)
    notify_channel = layer._notify_channel(
        "specific.1234567890abcdef1234567890abcdef1234567890abcdef"
    )
    assert len(notify_channel) <= 63
    assert notify_channel.startswith("fastapi_websockets_")


def test_prunes_expired_messages_and_group_members() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False, prune_interval=0.001)
        now = datetime.now(timezone.utc)
        pool.messages.append(
            {
                "id": 1,
                "channel": "chat.room",
                "payload": '{"type":"stale"}',
                "expires_at": now,
            }
        )
        pool.group_members[("room", "channel.old")] = now

        await asyncio.sleep(0.01)
        await layer.send("chat.room", {"type": "message", "text": "fresh"})

        assert len(pool.messages) == 1
        assert pool.messages[0]["payload"] == '{"text":"fresh","type":"message"}'
        assert pool.group_members == {}

    asyncio.run(run())


def test_receive_uses_listen_notify_wakeup_when_pool_supports_listeners() -> None:
    async def run() -> None:
        pool = FakePostgresPool()
        layer = PostgreSQLChannelLayer(pool=pool, ensure_schema=False, poll_interval=1.0)
        task = asyncio.create_task(layer.receive("chat.room", timeout=0.2))
        await asyncio.sleep(0.01)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        notify_channel = layer._notify_channel("chat.room")
        for connection in pool.listener_connections:
            connection.notify(notify_channel, "chat.room")
        message = await task
        assert message == {"type": "message", "text": "hello"}

    asyncio.run(run())
