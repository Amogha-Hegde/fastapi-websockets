import asyncio

from fastapi import WebSocketDisconnect

from fastapi_websockets.backends.nats import NATSChannelLayer
from fastapi_websockets.consumers import AsyncJsonWebSocketConsumer, AsyncWebSocketConsumer
from fastapi_websockets.exceptions import ChannelLayerClosed
from fastapi_websockets.messages import websocket_json_message


class FakeWebSocket:
    def __init__(self, events, path_params=None, query_params=None) -> None:
        self.events = list(events)
        self.accepted = False
        self.closed = False
        self.close_code = None
        self.close_reason = None
        self.sent_text = []
        self.sent_bytes = []
        self.sent_json = []
        self.accept_subprotocol = None
        self.accept_headers = None
        self.scope = {"type": "websocket"}
        self.path_params = path_params or {}
        self.query_params = query_params or {}

    async def accept(self, subprotocol=None, headers=None) -> None:
        self.accepted = True
        self.accept_subprotocol = subprotocol
        self.accept_headers = headers

    async def close(self, code=1000, reason=None) -> None:
        self.closed = True
        self.close_code = code
        self.close_reason = reason

    async def receive(self):
        await asyncio.sleep(0)
        if not self.events:
            raise WebSocketDisconnect(code=1000)
        item = self.events.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def send_text(self, text: str) -> None:
        self.sent_text.append(text)

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)

    async def send_json(self, data) -> None:
        self.sent_json.append(data)


class RecordingConsumer(AsyncWebSocketConsumer):
    channel_name_prefix = "ws"

    def __init__(self, layer) -> None:
        super().__init__(layer=layer)
        self.connected = False
        self.disconnected_code = None
        self.received_text = []
        self.received_bytes = []
        self.custom_events = []

    async def connect(self) -> None:
        self.connected = True
        await self.group_add("room")
        await self.accept()

    async def disconnect(self, close_code: int | None) -> None:
        self.disconnected_code = close_code

    async def receive_text(self, text_data: str) -> None:
        self.received_text.append(text_data)
        await self.channel_layer.send(
            self.channel_name,
            websocket_json_message({"echo": text_data}),
        )

    async def receive_bytes(self, bytes_data: bytes) -> None:
        self.received_bytes.append(bytes_data)
        await self.channel_layer.send(
            self.channel_name,
            {"type": "websocket.send", "mode": "bytes", "body": bytes_data},
        )

    async def send_back(self, event) -> None:
        self.custom_events.append(event)
        await self.send_json(event["data"])


class RecordingJsonConsumer(AsyncJsonWebSocketConsumer):
    channel_name_prefix = "json"

    def __init__(self, layer) -> None:
        super().__init__(layer=layer)
        self.payloads = []
        self.binary_payloads = []

    async def receive_json(self, content) -> None:
        self.payloads.append(content)
        await self.send_json({"ok": content})

    async def receive_bytes(self, bytes_data: bytes) -> None:
        self.binary_payloads.append(bytes_data)


def test_async_websocket_consumer_handles_text_bytes_and_channel_events() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    async def run() -> None:
        layer = InMemoryChannelLayer()
        websocket = FakeWebSocket(
            [
                {"type": "websocket.receive", "text": "hello"},
                {"type": "websocket.receive", "bytes": b"\x00\x01"},
                {"type": "websocket.disconnect", "code": 1001},
            ],
            path_params={"tenant": "acme"},
            query_params={"debug": "1"},
        )
        consumer = RecordingConsumer(layer)

        async def push_group_event() -> None:
            while not consumer.channel_name:
                await asyncio.sleep(0)
            while "room" not in consumer._joined_groups:
                await asyncio.sleep(0)
            await layer.group_send("room", {"type": "send.back", "data": {"group": "ok"}})

        await asyncio.gather(consumer(websocket), push_group_event())

        assert websocket.accepted is True
        assert consumer.connected is True
        assert consumer.disconnected_code == 1001
        assert consumer.received_text == ["hello"]
        assert consumer.received_bytes == [b"\x00\x01"]
        assert len(websocket.sent_json) == 2
        assert {"echo": "hello"} in websocket.sent_json
        assert {"group": "ok"} in websocket.sent_json
        assert websocket.sent_bytes == [b"\x00\x01"]
        assert consumer.path_params == {"tenant": "acme"}
        assert consumer.query_params == {"debug": "1"}

    asyncio.run(run())


def test_async_json_websocket_consumer_parses_json_text() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    async def run() -> None:
        layer = InMemoryChannelLayer()
        websocket = FakeWebSocket(
            [
                {"type": "websocket.receive", "text": '{"event":"ping"}'},
                {"type": "websocket.receive", "bytes": b"raw"},
                {"type": "websocket.disconnect", "code": 1000},
            ]
        )
        consumer = RecordingJsonConsumer(layer)
        await consumer(websocket)

        assert websocket.accepted is True
        assert consumer.payloads == [{"event": "ping"}]
        assert consumer.binary_payloads == [b"raw"]
        assert websocket.sent_json == [{"ok": {"event": "ping"}}]

    asyncio.run(run())


def test_async_websocket_consumer_auto_accepts_when_connect_does_not() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    class PassiveConsumer(AsyncWebSocketConsumer):
        async def connect(self) -> None:
            return None

    async def run() -> None:
        layer = InMemoryChannelLayer()
        websocket = FakeWebSocket([{"type": "websocket.disconnect", "code": 1000}])
        consumer = PassiveConsumer(layer)
        await consumer(websocket)
        assert websocket.accepted is True

    asyncio.run(run())


def test_async_websocket_consumer_tolerates_idle_nats_polling() -> None:
    class FakeSubscription:
        async def fetch(self, batch: int, timeout: float):
            await asyncio.sleep(0)
            raise TimeoutError

        async def unsubscribe(self) -> None:
            return None

    class FakeJetStream:
        async def pull_subscribe(self, subject: str, durable: str):
            del subject, durable
            return FakeSubscription()

    class DelayedDisconnectWebSocket(FakeWebSocket):
        async def receive(self):
            await asyncio.sleep(0.02)
            return {"type": "websocket.disconnect", "code": 1000}

    class PassiveConsumer(AsyncWebSocketConsumer):
        async def connect(self) -> None:
            return None

    async def run() -> None:
        layer = NATSChannelLayer(jetstream=FakeJetStream(), message_timeout=0.001)
        websocket = DelayedDisconnectWebSocket([])
        consumer = PassiveConsumer(layer)
        await consumer(websocket)
        assert websocket.accepted is True

    asyncio.run(run())


def test_async_websocket_consumer_ignores_closed_layer_during_group_cleanup() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    class ClosingLayerConsumer(AsyncWebSocketConsumer):
        async def connect(self) -> None:
            await self.group_add("room")
            await self.accept()

        async def receive_text(self, text_data: str) -> None:
            del text_data
            await self.channel_layer.close()
            await self.close()

    async def run() -> None:
        layer = InMemoryChannelLayer()
        websocket = FakeWebSocket(
            [
                {"type": "websocket.receive", "text": "shutdown"},
            ]
        )
        consumer = ClosingLayerConsumer(layer)
        await consumer(websocket)
        assert websocket.accepted is True

    asyncio.run(run())


def test_async_websocket_consumer_ignores_closed_layer_in_disconnect_hook() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    class DisconnectUsesLayerConsumer(AsyncWebSocketConsumer):
        def __init__(self, layer) -> None:
            super().__init__(layer=layer)
            self.disconnect_called = False

        async def connect(self) -> None:
            await self.accept()

        async def disconnect(self, close_code: int | None) -> None:
            del close_code
            self.disconnect_called = True
            try:
                await self.channel_layer.group_discard("room", self.channel_name)
            except ChannelLayerClosed:
                raise

    async def run() -> None:
        layer = InMemoryChannelLayer()
        websocket = FakeWebSocket([{"type": "websocket.disconnect", "code": 1000}])
        consumer = DisconnectUsesLayerConsumer(layer)

        async def close_layer() -> None:
            while not consumer.channel_name:
                await asyncio.sleep(0)
            await layer.close()

        await asyncio.gather(consumer(websocket), close_layer())
        assert consumer.disconnect_called is True

    asyncio.run(run())


def test_async_websocket_consumer_suppresses_transport_shutdown_errors() -> None:
    class TransportClosingLayer:
        def __init__(self) -> None:
            self.closed = False

        async def new_channel(self, prefix: str = "specific") -> str:
            return f"{prefix}.channel"

        async def group_add(self, group: str, channel: str) -> None:
            del group, channel

        async def receive(self, channel: str, timeout: float | None = None):
            del channel, timeout
            while not self.closed:
                await asyncio.sleep(0)
            raise ConnectionResetError("transport closing")

        async def group_discard(self, group: str, channel: str) -> None:
            del group, channel
            raise BrokenPipeError("socket closed")

        async def close(self) -> None:
            self.closed = True

    class ShutdownConsumer(AsyncWebSocketConsumer):
        def __init__(self, layer) -> None:
            super().__init__(layer=layer)
            self.disconnect_called = False

        async def connect(self) -> None:
            await self.group_add("room")
            await self.accept()

        async def disconnect(self, close_code: int | None) -> None:
            del close_code
            self.disconnect_called = True
            await self.channel_layer.close()
            raise OSError("transport already closed")

    async def run() -> None:
        layer = TransportClosingLayer()
        websocket = FakeWebSocket([{"type": "websocket.disconnect", "code": 1000}])
        consumer = ShutdownConsumer(layer)
        await consumer(websocket)
        assert consumer.disconnect_called is True

    asyncio.run(run())


def test_async_websocket_consumer_group_cleanup_still_surfaces_backend_bugs() -> None:
    class BuggyLayer:
        async def new_channel(self, prefix: str = "specific") -> str:
            return f"{prefix}.channel"

        async def group_add(self, group: str, channel: str) -> None:
            del group, channel

        async def receive(self, channel: str, timeout: float | None = None):
            del channel, timeout
            await asyncio.sleep(3600)

        async def group_discard(self, group: str, channel: str) -> None:
            del group, channel
            raise ValueError("group cleanup bug")

    class BuggyConsumer(AsyncWebSocketConsumer):
        async def connect(self) -> None:
            await self.group_add("room")
            await self.accept()

    async def run() -> None:
        consumer = BuggyConsumer(BuggyLayer())
        websocket = FakeWebSocket([{"type": "websocket.disconnect", "code": 1000}])
        try:
            await consumer(websocket)
        except ValueError as exc:
            assert str(exc) == "group cleanup bug"
        else:
            raise AssertionError("Expected backend cleanup failure to surface")

    asyncio.run(run())


def test_async_websocket_consumer_ignores_group_cleanup_backend_errors() -> None:
    from fastapi_websockets.backends.inmemory import InMemoryChannelLayer

    class FailingDiscardLayer(InMemoryChannelLayer):
        async def group_discard(self, group: str, channel: str) -> None:
            del group, channel
            raise RuntimeError("transport is shutting down")

    class CleanupConsumer(AsyncWebSocketConsumer):
        async def connect(self) -> None:
            self._joined_groups.add("room")
            await self.accept()

    async def run() -> None:
        layer = FailingDiscardLayer()
        websocket = FakeWebSocket([{"type": "websocket.disconnect", "code": 1000}])
        consumer = CleanupConsumer(layer)
        await consumer(websocket)
        assert websocket.accepted is True
        assert consumer._joined_groups == set()

    asyncio.run(run())
