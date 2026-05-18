from fastapi_websockets.backends.rabbitmq import RabbitMQChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed


class FakeRabbitMessage:
    def __init__(self, body: bytes):
        self.body = body
        self.acked = False

    async def ack(self):
        self.acked = True


class FakeQueue:
    def __init__(self, connection, name, arguments=None) -> None:
        self.connection = connection
        self.name = name
        self.arguments = arguments or None
        self.messages = []
        self.bindings = set()

    async def bind(self, exchange, routing_key) -> None:
        self.bindings.add((exchange.name, routing_key))
        self.connection.bindings.setdefault(exchange.name, set()).add((self.name, routing_key))

    async def unbind(self, exchange, routing_key) -> None:
        self.bindings.discard((exchange.name, routing_key))
        bound = self.connection.bindings.get(exchange.name)
        if bound is None:
            return
        bound.discard((self.name, routing_key))
        if not bound:
            self.connection.bindings.pop(exchange.name, None)

    async def get(self, timeout=None, fail=False):
        if not self.messages:
            return None
        return self.messages.pop(0)


class FakeExchange:
    def __init__(self, connection, name: str, exchange_type: str) -> None:
        self.connection = connection
        self.name = name
        self.exchange_type = exchange_type

    async def publish(self, message, routing_key: str) -> None:
        bindings = self.connection.bindings.get(self.name, set())
        for queue_name, bound_routing_key in bindings:
            if self.exchange_type == "fanout" or bound_routing_key == routing_key:
                self.connection.queues[queue_name].messages.append(FakeRabbitMessage(message.body))


class FakeChannel:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.closed = False

    async def declare_exchange(self, name, exchange_type, durable=True):
        exchange = self.connection.exchanges.get(name)
        if exchange is None:
            exchange = FakeExchange(self.connection, name, str(exchange_type).lower())
            self.connection.exchanges[name] = exchange
        return exchange

    async def declare_queue(self, name, durable=True, arguments=None):
        queue = self.connection.queues.get(name)
        if queue is None:
            queue = FakeQueue(self.connection, name, arguments=arguments)
            self.connection.queues[name] = queue
        return queue

    async def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.queues = {}
        self.exchanges = {}
        self.bindings = {}
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


def test_send_and_receive_round_trip_with_bytes() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
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
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
        layer._ensure_group_exchange = fake_ensure_group_exchange.__get__(layer, RabbitMQChannelLayer)
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")
        await layer.group_send("room", {"type": "broadcast", "text": "hi"})
        first = await layer.receive("channel.one", timeout=0.1)
        second = await layer.receive("channel.two", timeout=0.1)
        assert first["text"] == "hi"
        assert second["text"] == "hi"

    asyncio.run(run())


def test_group_membership_is_shared_across_layer_instances() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer_one = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer_two = RabbitMQChannelLayer(rabbitmq_connection=connection)
        for layer in (layer_one, layer_two):
            layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
            layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
            layer._ensure_group_exchange = fake_ensure_group_exchange.__get__(layer, RabbitMQChannelLayer)
        await layer_one.group_add("room", "channel.one")
        await layer_two.group_send("room", {"type": "broadcast", "text": "shared"})
        message = await layer_one.receive("channel.one", timeout=0.1)
        assert message == {"type": "broadcast", "text": "shared"}

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
        layer._ensure_group_exchange = fake_ensure_group_exchange.__get__(layer, RabbitMQChannelLayer)
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


def test_receive_without_timeout_waits_for_late_message() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(rabbitmq_connection=connection, poll_interval=0.001)
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)

        receive_task = asyncio.create_task(layer.receive("chat.room"))
        await asyncio.sleep(0.01)
        await layer.send("chat.room", {"type": "message", "text": "delayed"})

        message = await asyncio.wait_for(receive_task, timeout=0.1)
        assert message == {"type": "message", "text": "delayed"}

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
        layer._owns_connection = True
        await layer.close()
        assert connection.channel_instance.closed is True
        assert connection.closed is True

    asyncio.run(run())


def test_declared_queues_use_queue_expiry() -> None:
    import asyncio

    async def run() -> None:
        connection = FakeConnection()
        layer = RabbitMQChannelLayer(
            rabbitmq_connection=connection,
            queue_expiry=1234,
        )
        layer._declare_exchange = fake_declare_exchange.__get__(layer, RabbitMQChannelLayer)
        layer._build_message = fake_build_message.__get__(layer, RabbitMQChannelLayer)
        await layer.send("chat.room", {"type": "message"})
        queue = connection.queues["fastapi-websockets.chat.room"]
        assert queue.arguments["x-expires"] == 1234

    asyncio.run(run())


async def fake_declare_exchange(self):
    return await self._channel.declare_exchange(self.exchange_name, "direct", durable=self.durable)


async def fake_build_message(self, payload: bytes):
    return FakeOutgoingMessage(payload)


async def fake_ensure_group_exchange(self, group: str):
    await self._get_channel()
    exchange_name = self._group_exchange_name(group)
    exchange = self._group_exchanges.get(exchange_name)
    if exchange is not None:
        return exchange
    exchange = await self._channel.declare_exchange(exchange_name, "fanout", durable=self.durable)
    self._group_exchanges[exchange_name] = exchange
    return exchange
