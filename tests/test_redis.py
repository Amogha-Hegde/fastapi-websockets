import asyncio
from collections import defaultdict
import sys
from types import ModuleType, SimpleNamespace

from fastapi_websockets.backends.redis import RedisChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed, InvalidChannelLayerConfig


class FakeRedis:
    def __init__(self) -> None:
        self.lists = defaultdict(list)
        self.sets = defaultdict(set)
        self.expiry = {}
        self.notifications = []
        self.closed = False
        self.pubsubs = []

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

    async def lpop(self, key: str):
        values = self.lists[key]
        if values:
            return values.pop(0)
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
        for pubsub in self.pubsubs:
            pubsub.push_message(channel, payload)
        return 1

    def pubsub(self):
        pubsub = FakePubSub(self)
        self.pubsubs.append(pubsub)
        return pubsub

    async def aclose(self) -> None:
        self.closed = True


class FakePubSub:
    def __init__(self, redis: FakeRedis) -> None:
        self.redis = redis
        self.subscriptions = set()
        self.messages = asyncio.Queue()
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        self.subscriptions.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        self.subscriptions.discard(channel)

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float | None = None):
        del ignore_subscribe_messages
        if timeout is None:
            return await self.messages.get()
        try:
            return await asyncio.wait_for(self.messages.get(), timeout=timeout)
        except TimeoutError:
            return None

    async def aclose(self) -> None:
        self.closed = True
        self.redis.pubsubs.remove(self)

    def push_message(self, channel: str, payload: bytes) -> None:
        if channel not in self.subscriptions:
            return
        self.messages.put_nowait({"channel": channel, "data": payload})


class FakeRedisWithClose:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeRedisNoHelpers:
    async def blpop(self, key: str, timeout: int = 0):
        del key, timeout
        return None


class FakePubSubWithoutSubscribe:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class FakeShardedPubSub(FakePubSub):
    async def ssubscribe(self, channel: str) -> None:
        await self.subscribe(channel)

    async def sunsubscribe(self, channel: str) -> None:
        await self.unsubscribe(channel)


class FakeShardedRedis(FakeRedis):
    def pubsub(self):
        pubsub = FakeShardedPubSub(self)
        self.pubsubs.append(pubsub)
        return pubsub


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


def test_receive_uses_pubsub_wakeup_when_available() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis, use_pubsub=True, sharded_pubsub=False)
        task = asyncio.create_task(layer.receive("chat.room", timeout=0.2))
        await asyncio.sleep(0)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await task
        assert message == {"type": "message", "text": "hello"}
        assert any(item[0] == "publish" for item in redis.notifications)

    asyncio.run(run())


def test_receive_without_pubsub_uses_blpop_timeout() -> None:
    async def run() -> None:
        redis = FakeRedis()
        layer = RedisChannelLayer(redis_client=redis, use_pubsub=False)
        try:
            await layer.receive("chat.room", timeout=0.01)
        except TimeoutError:
            pass
        else:
            raise AssertionError("Expected TimeoutError")

    asyncio.run(run())


def test_group_send_decodes_byte_channel_members() -> None:
    async def run() -> None:
        redis = FakeRedis()
        redis.sets["fastapi-websockets:group:room:members"].add(b"channel.one")
        layer = RedisChannelLayer(redis_client=redis)
        await layer.group_send("room", {"type": "broadcast", "text": "hi"})
        message = await layer.receive("channel.one", timeout=1)
        assert message["text"] == "hi"

    asyncio.run(run())


def test_close_uses_sync_close_when_aclose_is_missing() -> None:
    async def run() -> None:
        redis = FakeRedisNoHelpers()
        closer = FakeRedisWithClose()
        layer = RedisChannelLayer()
        layer._redis = closer
        layer._owns_client = True
        await layer.close()
        assert closer.closed is True
        del redis

    asyncio.run(run())


def test_wait_for_notification_returns_false_without_pubsub_support() -> None:
    async def run() -> None:
        layer = RedisChannelLayer(redis_client=FakeRedisNoHelpers())
        result = await layer._wait_for_notification(layer._redis, "chat.room", 0.01)
        assert result is False

    asyncio.run(run())


def test_wait_for_notification_closes_pubsub_when_subscribe_api_is_missing() -> None:
    async def run() -> None:
        pubsub = FakePubSubWithoutSubscribe()
        client = SimpleNamespace(pubsub=lambda: pubsub)
        layer = RedisChannelLayer(redis_client=client)
        result = await layer._wait_for_notification(client, "chat.room", 0.01)
        assert result is False
        assert pubsub.closed is True

    asyncio.run(run())


def test_sharded_pubsub_notification_path_is_used() -> None:
    async def run() -> None:
        redis = FakeShardedRedis()
        layer = RedisChannelLayer(redis_client=redis, sharded_pubsub=True)
        await layer.send("chat.room", {"type": "message"})
        assert redis.notifications[0][0] == "spublish"
        task = asyncio.create_task(layer.receive("chat.room", timeout=0.2))
        await asyncio.sleep(0)
        await layer.send("chat.room", {"type": "message", "text": "wake"})
        message = await task
        assert message["type"] == "message"

    asyncio.run(run())


def test_pop_message_falls_back_to_lists_attribute() -> None:
    async def run() -> None:
        client = SimpleNamespace(lists=defaultdict(list))
        client.lists["queue"].append(b"payload")
        layer = RedisChannelLayer(redis_client=client)
        payload = await layer._pop_message(client, "queue")
        assert payload == b"payload"

    asyncio.run(run())


def test_pop_message_returns_none_without_helpers() -> None:
    async def run() -> None:
        layer = RedisChannelLayer(redis_client=FakeRedisNoHelpers())
        payload = await layer._pop_message(layer._redis, "queue")
        assert payload is None

    asyncio.run(run())


def test_get_client_builds_standard_and_cluster_clients() -> None:
    async def run() -> None:
        redis_module = ModuleType("redis")
        asyncio_module = ModuleType("redis.asyncio")
        cluster_module = ModuleType("redis.asyncio.cluster")

        class Redis:
            @staticmethod
            def from_url(url: str):
                return {"kind": "redis", "url": url}

        class RedisCluster:
            @staticmethod
            def from_url(url: str):
                return {"kind": "cluster", "url": url}

        asyncio_module.Redis = Redis
        cluster_module.RedisCluster = RedisCluster

        old_modules = {name: sys.modules.get(name) for name in ("redis", "redis.asyncio", "redis.asyncio.cluster")}
        sys.modules["redis"] = redis_module
        sys.modules["redis.asyncio"] = asyncio_module
        sys.modules["redis.asyncio.cluster"] = cluster_module
        try:
            standard = RedisChannelLayer(redis_client=None, cluster=False)
            clustered = RedisChannelLayer(redis_client=None, cluster=True)
            assert await standard._get_client() == {"kind": "redis", "url": standard.url}
            assert await clustered._get_client() == {"kind": "cluster", "url": clustered.url}
        finally:
            for name, module in old_modules.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module

    asyncio.run(run())


def test_get_client_raises_invalid_config_when_dependency_is_missing() -> None:
    async def run() -> None:
        old_modules = {name: sys.modules.get(name) for name in ("redis", "redis.asyncio", "redis.asyncio.cluster")}
        sys.modules.pop("redis", None)
        sys.modules.pop("redis.asyncio", None)
        sys.modules.pop("redis.asyncio.cluster", None)
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("redis.asyncio"):
                raise ImportError("missing redis")
            return original_import(name, globals, locals, fromlist, level)

        import builtins

        builtins_import = builtins.__import__
        builtins.__import__ = fake_import
        try:
            layer = RedisChannelLayer(redis_client=None)
            try:
                await layer._get_client()
            except InvalidChannelLayerConfig as exc:
                assert "optional dependency group" in str(exc)
            else:
                raise AssertionError("Expected InvalidChannelLayerConfig")
        finally:
            builtins.__import__ = builtins_import
            for name, module in old_modules.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module

    asyncio.run(run())


def test_key_builders_and_name_validation() -> None:
    layer = RedisChannelLayer(redis_client=FakeRedis())
    assert layer._channel_queue_key("room") == "fastapi-websockets:channel:{room}:queue"
    assert layer._notify_channel("room") == "fastapi-websockets:channel:{room}:notify"
    assert layer._group_key("room") == "fastapi-websockets:group:room:members"
    try:
        layer._validate_name("channel", "")
    except ValueError as exc:
        assert "Channel name" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
