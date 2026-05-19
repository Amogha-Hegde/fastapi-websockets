from __future__ import annotations

import asyncio
from typing import Any, Mapping
from uuid import uuid4

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed, InvalidChannelLayerConfig
from fastapi_websockets.serialization import JsonSerializer


class RabbitMQChannelLayer(BaseChannelLayer):
    """RabbitMQ-backed channel layer using aio-pika exchanges and quorum queues."""

    def __init__(
        self,
        url: str = "amqp://guest:guest@localhost:5672//",
        exchange_name: str = "fastapi_websockets",
        queue_prefix: str = "fastapi-websockets",
        durable: bool = True,
        message_ttl: int | None = 60000,
        queue_expiry: int | None = 300000,
        poll_interval: float = 0.1,
        rabbitmq_connection: Any | None = None,
        serializer: Any | None = None,
        **config: Any,
    ) -> None:
        if not durable:
            raise InvalidChannelLayerConfig(
                "RabbitMQ quorum queues must be durable"
            )
        super().__init__(
            url=url,
            exchange_name=exchange_name,
            queue_prefix=queue_prefix,
            durable=durable,
            message_ttl=message_ttl,
            queue_expiry=queue_expiry,
            poll_interval=poll_interval,
            **config,
        )
        self.url = url
        self.exchange_name = exchange_name
        self.queue_prefix = queue_prefix
        self.durable = durable
        self.message_ttl = message_ttl
        self.queue_expiry = queue_expiry
        self.poll_interval = poll_interval
        self.serializer = serializer or JsonSerializer()
        self._connection = rabbitmq_connection
        self._owns_connection = rabbitmq_connection is None
        self._closed = False
        self._channel = None
        self._exchange = None
        self._declared_queues: dict[str, Any] = {}
        self._group_exchanges: dict[str, Any] = {}

    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("channel", channel)
        await self._ensure_queue(channel)
        message_payload = self.serializer.dumps(message)
        outgoing = await self._build_message(message_payload)
        await self._exchange.publish(outgoing, routing_key=self._routing_key(channel))

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        self._ensure_open()
        self._validate_name("channel", channel)
        queue = await self._ensure_queue(channel)
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            wait_for = self.poll_interval
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'")
                wait_for = min(wait_for, remaining)

            try:
                message = await queue.get(timeout=wait_for, fail=False)
            except BaseException as exc:
                if self._is_expected_receive_shutdown_error(exc):
                    raise ChannelLayerClosed("RabbitMQ channel layer is shutting down") from exc
                raise
            if message is None:
                if deadline is not None and loop.time() >= deadline:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'")
                await asyncio.sleep(0)
                continue

            await message.ack()
            return self.serializer.loads(message.body)

    async def new_channel(self, prefix: str = "specific") -> str:
        self._ensure_open()
        self._validate_name("channel", prefix)
        return f"{prefix}.{uuid4().hex}"

    async def group_add(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        queue = await self._ensure_queue(channel)
        exchange = await self._ensure_group_exchange(group)
        await queue.bind(exchange, routing_key="")

    async def group_discard(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        queue = await self._ensure_queue(channel)
        exchange = await self._ensure_group_exchange(group)
        unbind = getattr(queue, "unbind", None)
        if unbind is None:
            return
        await unbind(exchange, routing_key="")

    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        exchange = await self._ensure_group_exchange(group)
        message_payload = self.serializer.dumps(message)
        outgoing = await self._build_message(message_payload)
        await exchange.publish(outgoing, routing_key="")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._channel is not None:
            await self._channel.close()
        if self._connection is not None and self._owns_connection:
            await self._connection.close()

    async def _get_connection(self) -> Any:
        if self._connection is not None:
            return self._connection
        try:
            import aio_pika
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "RabbitMQ backend requires the optional dependency group: pip install 'fastapi-websockets[rabbitmq]'"
            ) from exc
        self._connection = await aio_pika.connect_robust(self.url)
        return self._connection

    async def _get_channel(self) -> Any:
        if self._channel is not None:
            return self._channel
        connection = await self._get_connection()
        self._channel = await connection.channel()
        self._exchange = await self._declare_exchange()
        return self._channel

    async def _declare_exchange(self) -> Any:
        try:
            import aio_pika
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "RabbitMQ backend requires the optional dependency group: pip install 'fastapi-websockets[rabbitmq]'"
            ) from exc
        return await self._channel.declare_exchange(
            self.exchange_name,
            aio_pika.ExchangeType.DIRECT,
            durable=self.durable,
        )

    async def _ensure_group_exchange(self, group: str) -> Any:
        await self._get_channel()
        exchange_name = self._group_exchange_name(group)
        exchange = self._group_exchanges.get(exchange_name)
        if exchange is not None:
            return exchange
        try:
            import aio_pika
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "RabbitMQ backend requires the optional dependency group: pip install 'fastapi-websockets[rabbitmq]'"
            ) from exc
        exchange = await self._channel.declare_exchange(
            exchange_name,
            aio_pika.ExchangeType.FANOUT,
            durable=self.durable,
        )
        self._group_exchanges[exchange_name] = exchange
        return exchange

    async def _ensure_queue(self, channel: str) -> Any:
        await self._get_channel()
        queue_name = self._queue_name(channel)
        queue = self._declared_queues.get(queue_name)
        if queue is not None:
            return queue
        queue = await self._build_queue(queue_name, channel)
        await queue.bind(self._exchange, routing_key=self._routing_key(channel))
        self._declared_queues[queue_name] = queue
        return queue

    async def _build_queue(self, queue_name: str, channel: str) -> Any:
        del channel
        queue_arguments = {"x-queue-type": "quorum"}
        if self.message_ttl is not None:
            queue_arguments["x-message-ttl"] = self.message_ttl
        if self.queue_expiry is not None:
            queue_arguments["x-expires"] = self.queue_expiry
        return await self._channel.declare_queue(
            queue_name,
            durable=self.durable,
            arguments=queue_arguments or None,
        )

    async def _build_message(self, payload: bytes) -> Any:
        try:
            import aio_pika
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "RabbitMQ backend requires the optional dependency group: pip install 'fastapi-websockets[rabbitmq]'"
            ) from exc
        kwargs = {"content_type": "application/json"}
        if self.message_ttl is not None:
            kwargs["expiration"] = self.message_ttl / 1000
        return aio_pika.Message(body=payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT if self.durable else aio_pika.DeliveryMode.NOT_PERSISTENT, **kwargs)

    def _routing_key(self, channel: str) -> str:
        return channel.replace(" ", "_")

    def _queue_name(self, channel: str) -> str:
        return f"{self.queue_prefix}.{channel}".replace(" ", "_")

    def _group_exchange_name(self, group: str) -> str:
        return f"{self.queue_prefix}.group.{group}".replace(" ", "_")

    def _ensure_open(self) -> None:
        if self._closed:
            raise ChannelLayerClosed("Channel layer has been closed")

    @staticmethod
    def _validate_name(kind: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{kind.title()} name must be a non-empty string")

    @classmethod
    def _is_expected_receive_shutdown_error(cls, exc: BaseException) -> bool:
        return cls._exception_chain_contains(
            exc,
            (asyncio.CancelledError, ChannelLayerClosed, ConnectionError, BrokenPipeError, EOFError, OSError),
        )

    @classmethod
    def _exception_chain_contains(
        cls,
        exc: BaseException,
        expected: tuple[type[BaseException], ...],
        seen: set[int] | None = None,
    ) -> bool:
        if isinstance(exc, expected):
            return True

        if seen is None:
            seen = set()
        marker = id(exc)
        if marker in seen:
            return False
        seen.add(marker)

        for nested in (getattr(exc, "__cause__", None), getattr(exc, "__context__", None)):
            if isinstance(nested, BaseException) and cls._exception_chain_contains(
                nested,
                expected,
                seen,
            ):
                return True
        return False
