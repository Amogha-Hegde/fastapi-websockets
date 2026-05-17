import asyncio
from collections import defaultdict

from fastapi_websockets.backends.redis import RedisChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed


class FakeRedis:
    def __init__(self) -> None:
        self.lists = defaultdict(list)
        self.sets = defaultdict(set)
        self.expiry = {}
        self.notifications = []
        self.closed = False

    async def rpush(self, key: str, value: bytes) -> int:
        self.lists[key].append(value)
        return len(self.lists[key])

    async def blpop(self, key: str, timeout: int = 0):
        values = self.lists[key]
        if values:
            return key, values.pop(0)
        if timeout == 0:
            raise AssertionError("FakeRedis cannot block forever in tests")
        return None

    async def expire(self, key: str, seconds: int) -> bool:
        self.expiry[key] = seconds
        return True

    async def sadd(self, key: str, value: str) -> int:
        before = len(self.sets[key])
        self.sets[key].add(value)
        return int(len(self.sets[key]) != before)

    async def srem(self, key: str, value: str) -> int:
        existed = value in self.sets[key]
        self.sets[key].discard(value)
        return int(existed)

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets[key])

    async def spublish(self, channel: str, payload: bytes) -> int:
        self.notifications.append(("spublish", channel, payload))
        return 1

    async def publish(self, channel: str, payload: bytes) -> int:
        self.notifications.append(("publish", channel, payload))
        return 1

    async def aclose(self) -> None:
        self.closed = True


def test_send_and_receive_round_trip() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await layer.receive("chat.room", timeout=1)
        assert message == {"type": "message", "text": "hello"}
        assert redis.notifications[0][0] == "spublish"

    asyncio.run(run())


def test_send_and_receive_round_trip_with_bytes() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis)
        payload = {
            "type": "websocket.send",
            "mode": "bytes",
            "body": b"\x00\x01hello",
        }
        await layer.send("chat.room", payload)
        message = await layer.receive("chat.room", timeout=1)
        assert message == payload

    asyncio.run(run())


def test_group_send_fans_out_to_group_members() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis)
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")

        await layer.group_send("room", {"type": "broadcast", "text": "hi"})

        first = await layer.receive("channel.one", timeout=1)
        second = await layer.receive("channel.two", timeout=1)
        assert first["text"] == "hi"
        assert second["text"] == "hi"

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis)
        await layer.group_add("room", "channel.one")
        await layer.group_discard("room", "channel.one")

        await layer.group_send("room", {"type": "broadcast"})
        try:
            await layer.receive("channel.one", timeout=1)
        except TimeoutError:
            pass
        else:
            raise AssertionError("Expected timeout after group_discard")

    asyncio.run(run())


def test_close_rejects_new_operations() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis)
        await layer.close()
        assert redis.closed is False
        try:
            await layer.send("chat.room", {"type": "message"})
        except ChannelLayerClosed:
            pass
        else:
            raise AssertionError("Expected ChannelLayerClosed after close")

    asyncio.run(run())


def test_close_closes_internal_client() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer()
        layer._redis = redis
        layer._owns_client = True
        await layer.close()
        assert redis.closed is True

    asyncio.run(run())
