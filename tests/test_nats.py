import asyncio

from fastapi_websockets.backends.nats import NATSChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed


class FakeNatsMessage:
    def __init__(self, data: bytes) -> None:
        self.data = data


class FakeSubscription:
    def __init__(self, stream_messages: list) -> None:
        self.unsubscribed = False
        self.stream_messages = stream_messages

    async def unsubscribe(self) -> None:
        self.unsubscribed = True

    async def fetch(self, batch: int, timeout: float):
        await asyncio.sleep(0)
        if not self.stream_messages:
            raise TimeoutError
        items = self.stream_messages[:batch]
        del self.stream_messages[:batch]
        return items


class FakeNatsTimeoutError(Exception):
    pass


FakeNatsTimeoutError.__module__ = "nats.errors"


class PollingSubscription:
    def __init__(self, stream_messages: list, failures_before_message: int = 0) -> None:
        self.stream_messages = stream_messages
        self.failures_before_message = failures_before_message

    async def fetch(self, batch: int, timeout: float):
        await asyncio.sleep(0)
        if self.failures_before_message > 0:
            self.failures_before_message -= 1
            raise FakeNatsTimeoutError()
        return self.stream_messages[:batch]


class FakeJetStreamMessage:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


class FakeKVEntry:
    def __init__(self, value: bytes, revision: int) -> None:
        self.value = value
        self.revision = revision


class FakeKVConflictError(Exception):
    pass


class FakeKVStore:
    def __init__(self) -> None:
        self.store = {}
        self.revisions = {}

    async def put(self, key: str, value: bytes) -> None:
        await asyncio.sleep(0)
        self.store[key] = value
        self.revisions[key] = self.revisions.get(key, 0) + 1

    async def create(self, key: str, value: bytes) -> None:
        await asyncio.sleep(0)
        if key in self.store:
            raise FakeKVConflictError(key)
        self.store[key] = value
        self.revisions[key] = 1

    async def update(self, key: str, value: bytes, revision: int) -> None:
        await asyncio.sleep(0)
        if self.revisions.get(key) != revision:
            raise FakeKVConflictError(key)
        self.store[key] = value
        self.revisions[key] = revision + 1

    async def get(self, key: str):
        await asyncio.sleep(0)
        if key not in self.store:
            raise KeyError(key)
        return FakeKVEntry(self.store[key], self.revisions[key])

    async def delete(self, key: str, revision: int | None = None) -> None:
        await asyncio.sleep(0)
        if revision is not None and self.revisions.get(key) != revision:
            raise FakeKVConflictError(key)
        self.store.pop(key, None)
        self.revisions.pop(key, None)


class FakeJetStream:
    def __init__(self, kv_store: FakeKVStore) -> None:
        self.kv_store = kv_store
        self.streams = []
        self.subscriptions = {}
        self.messages = {}

    async def create_key_value(self, bucket: str):
        return self.kv_store

    async def key_value(self, bucket: str):
        return self.kv_store

    async def add_stream(self, name: str, subjects: list[str]):
        self.streams.append((name, tuple(subjects)))

    async def publish(self, subject: str, payload: bytes) -> None:
        self.messages.setdefault(subject, []).append(FakeJetStreamMessage(payload))

    async def pull_subscribe(self, subject: str, durable: str):
        subscription = self.subscriptions.get(subject)
        if subscription is None:
            subscription = FakeSubscription(self.messages.setdefault(subject, []))
            self.subscriptions[subject] = subscription
        return subscription


class FakeNatsClient:
    def __init__(self) -> None:
        self.drained = False
        self.closed = False
        self.kv_store = FakeKVStore()
        self.js = FakeJetStream(self.kv_store)

    def jetstream(self) -> FakeJetStream:
        return self.js

    async def drain(self) -> None:
        self.drained = True

    async def close(self) -> None:
        self.closed = True


def test_send_and_receive_round_trip() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == {"type": "message", "text": "hello"}

    asyncio.run(run())


def test_send_and_receive_round_trip_with_bytes() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        payload = {
            "type": "websocket.send",
            "mode": "bytes",
            "body": b"\x00\x01hello",
        }
        await layer.send("chat.room", payload)
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == payload

    asyncio.run(run())


def test_group_send_fans_out_to_group_members() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")
        await layer.group_send("room", {"type": "broadcast", "text": "hi"})
        first = await layer.receive("channel.one", timeout=0.1)
        second = await layer.receive("channel.two", timeout=0.1)
        assert first["text"] == "hi"
        assert second["text"] == "hi"

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
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
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        await layer.close()
        assert nc.closed is False
        try:
            await layer.send("chat.room", {"type": "message"})
        except ChannelLayerClosed:
            pass
        else:
            raise AssertionError("Expected ChannelLayerClosed after close")

    asyncio.run(run())


def test_close_closes_internal_client() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer()
        layer._nc = nc
        layer._owns_client = True
        await layer.close()
        assert nc.drained is True
        assert nc.closed is True

    asyncio.run(run())


def test_receive_normalizes_nats_timeout_errors() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        subject = layer._channel_subject("chat.room")
        nc.js.subscriptions[subject] = PollingSubscription([], failures_before_message=1)
        try:
            await layer.receive("chat.room", timeout=0.01)
        except TimeoutError:
            pass
        else:
            raise AssertionError("Expected built-in TimeoutError for idle NATS receive")

    asyncio.run(run())


def test_concurrent_group_add_preserves_all_members() -> None:
    async def run() -> None:
        nc = FakeNatsClient()
        layer = NATSChannelLayer(nats_client=nc)
        await asyncio.gather(
            layer.group_add("room", "channel.one"),
            layer.group_add("room", "channel.two"),
        )
        channels = await layer._get_group_channels("room")
        assert channels == {"channel.one", "channel.two"}

    asyncio.run(run())
