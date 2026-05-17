import asyncio

from fastapi import WebSocketDisconnect

from fastapi_websockets.consumers import AsyncJsonWebSocketConsumer, AsyncWebSocketConsumer
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
