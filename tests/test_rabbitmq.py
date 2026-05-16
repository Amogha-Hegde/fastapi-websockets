from fastapi_websockets.backends.rabbitmq import RabbitMQChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed


class FakeRabbitMessage:
    def __init__(self, body: bytes):
        self.body = body
        self.acked = False

    async def ack(self):
        self.acked = True


class FakeQueue:
    def __init__(self, connection, name, routing_key) -> None:
        self.connection = connection
        self.name = name
        self.routing_key = routing_key
        self.declared = False
        self.bound = False

    async def bind(self, exchange, routing_key) -> None:
        self.bound = True

    async def get(self, timeout=None, fail=False):
        messages = self.connection.messages.get(self.routing_key, [])
        if not messages:
            return None
        return messages.pop(0)


class FakeExchange:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def publish(self, message, routing_key: str) -> None:
        self.connection.messages.setdefault(routing_key, []).append(FakeRabbitMessage(message.body))


class FakeChannel:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.closed = False
        self.exchange = FakeExchange(connection)

    async def declare_exchange(self, name, exchange_type, durable=True):
        return self.exchange

    async def declare_queue(self, name, durable=True, arguments=None):
        return FakeQueue(self.connection, name, name.split(".", 1)[1])

    async def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.messages = {}
        self.channel_instance = FakeChannel(self)

    async def channel(self):
        return self.channel_instance

    async def close(self) -> None:
        self.closed = True


class FakeOutgoingMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body


def test_send_and_receive_round_trip() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == {"type": "message", "text": "hello"}

    asyncio.run(run())


def test_group_send_fans_out_to_group_members() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")
        await layer.group_send("room", {"type": "broadcast", "text": "hi"})
        first = await layer.receive("channel.one", timeout=0.1)
        second = await layer.receive("channel.two", timeout=0.1)
        assert first["text"] == "hi"
        assert second["text"] == "hi"

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
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
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        await layer.close()
        assert connection.closed is False
        try:
            await layer.send("chat.room", {"type": "message"})
        except ChannelLayerClosed:
            pass
        else:
            raise AssertionError("Expected ChannelLayerClosed after close")

    asyncio.run(run())


def test_close_closes_internal_connection() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer()
        layer._connection = connection
        layer._channel = connection.channel_instance
        layer._exchange = connection.channel_instance.exchange
        layer._owns_connection = True
        await layer.close()
        assert connection.channel_instance.closed is True
        assert connection.closed is True

    asyncio.run(run())


async def fake_declare_exchange(self):
    return self._channel.exchange


async def fake_build_message(self, payload: bytes):
    return FakeOutgoingMessage(payload)
