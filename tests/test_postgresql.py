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
        except TimeoutError:
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
