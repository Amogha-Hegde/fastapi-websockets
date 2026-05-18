from __future__ import annotations

import asyncio
from typing import Any, Mapping
from uuid import uuid4

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed, InvalidChannelLayerConfig
from fastapi_websockets.serialization import JsonSerializer


class NATSChannelLayer(BaseChannelLayer):
    """NATS-backed channel layer using subjects for channels and KV for groups."""

    def __init__(
        self,
        servers: list[str] | None = None,
        prefix: str = "fastapi-websockets",
        group_bucket: str = "fastapi_websockets_groups",
        stream_name: str = "FASTAPI_WEBSOCKETS",
        message_timeout: float = 60.0,
        nats_client: Any | None = None,
        jetstream: Any | None = None,
        kv_store: Any | None = None,
        serializer: Any | None = None,
        **config: Any,
    ) -> None:
        super().__init__(
            servers=servers or ["nats://localhost:4222"],
            prefix=prefix,
            group_bucket=group_bucket,
            stream_name=stream_name,
            message_timeout=message_timeout,
            **config,
        )
        self.servers = servers or ["nats://localhost:4222"]
        self.prefix = prefix
        self.group_bucket = group_bucket
        self.stream_name = stream_name
        self.message_timeout = message_timeout
        self.serializer = serializer or JsonSerializer()
        self._nc = nats_client
        self._js = jetstream
        self._kv = kv_store
        self._owns_client = nats_client is None
        self._closed = False
        self._subscriptions: dict[str, Any] = {}
        self._stream_ready = jetstream is not None

    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("channel", channel)
        js = await self._get_jetstream()
        await js.publish(self._channel_subject(channel), self.serializer.dumps(message))

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        self._ensure_open()
        self._validate_name("channel", channel)
        subscription = await self._ensure_subscription(channel)
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            wait_for = self.message_timeout
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'")
                wait_for = min(wait_for, remaining)

            try:
                messages = await subscription.fetch(1, timeout=wait_for)
            except Exception as exc:
                if not self._is_receive_timeout(exc):
                    raise
                if deadline is not None and loop.time() >= deadline:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'") from exc
                continue

            if not messages:
                if deadline is not None and loop.time() >= deadline:
                    raise TimeoutError(f"Timed out waiting for channel '{channel}'")
                continue

            message = messages[0]
            payload = self.serializer.loads(message.data)
            ack = getattr(message, "ack", None)
            if ack is not None:
                result = ack()
                if result is not None:
                    await result
            return payload

    async def new_channel(self, prefix: str = "specific") -> str:
        self._ensure_open()
        self._validate_name("channel", prefix)
        return f"{prefix}.{uuid4().hex}"

    async def group_add(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        kv = await self._get_kv_store()
        channels = await self._get_group_channels(group)
        channels.add(channel)
        await kv.put(self._group_key(group), self.serializer.dumps({"channels": sorted(channels)}))

    async def group_discard(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        kv = await self._get_kv_store()
        channels = await self._get_group_channels(group)
        channels.discard(channel)
        if channels:
            await kv.put(self._group_key(group), self.serializer.dumps({"channels": sorted(channels)}))
            return
        delete = getattr(kv, "delete", None)
        if delete is not None:
            await delete(self._group_key(group))
        else:
            await kv.put(self._group_key(group), self.serializer.dumps({"channels": []}))

    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        channels = await self._get_group_channels(group)
        for channel in channels:
            await self.send(channel, message)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subscription in self._subscriptions.values():
            unsubscribe = getattr(subscription, "unsubscribe", None)
            if unsubscribe is not None:
                result = unsubscribe()
                if result is not None:
                    await result
        self._subscriptions.clear()
        if self._nc is not None and self._owns_client:
            drain = getattr(self._nc, "drain", None)
            if drain is not None:
                await drain()
            close = getattr(self._nc, "close", None)
            if close is not None:
                result = close()
                if result is not None:
                    await result

    async def _get_client(self) -> Any:
        if self._nc is not None:
            return self._nc
        try:
            import nats
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "NATS backend requires the optional dependency group: pip install 'fastapi-websockets[nats]'"
            ) from exc
        self._nc = await nats.connect(servers=self.servers)
        return self._nc

    async def _get_kv_store(self) -> Any:
        if self._kv is not None:
            return self._kv
        jetstream = await self._get_jetstream()
        create = getattr(jetstream, "create_key_value", None)
        if create is not None:
            try:
                self._kv = await create(bucket=self.group_bucket)
            except Exception:
                self._kv = await jetstream.key_value(self.group_bucket)
            return self._kv
        self._kv = await jetstream.key_value(self.group_bucket)
        return self._kv

    async def _get_jetstream(self) -> Any:
        if self._js is None:
            nc = await self._get_client()
            self._js = nc.jetstream()
        if not self._stream_ready:
            await self._ensure_stream()
            self._stream_ready = True
        return self._js

    async def _ensure_stream(self) -> None:
        js = self._js
        if js is None:
            raise InvalidChannelLayerConfig("JetStream context is not initialized")
        add_stream = getattr(js, "add_stream", None)
        if add_stream is None:
            return
        try:
            await add_stream(name=self.stream_name, subjects=[f"{self.prefix}.channel.*"])
        except Exception:
            pass

    async def _ensure_subscription(self, channel: str) -> Any:
        if channel in self._subscriptions:
            return self._subscriptions[channel]
        js = await self._get_jetstream()
        durable = self._durable_name(channel)
        self._subscriptions[channel] = await js.pull_subscribe(
            self._channel_subject(channel),
            durable=durable,
        )
        return self._subscriptions[channel]

    async def _get_group_channels(self, group: str) -> set[str]:
        kv = await self._get_kv_store()
        try:
            entry = await kv.get(self._group_key(group))
        except Exception:
            return set()
        payload = getattr(entry, "value", entry)
        if payload is None:
            return set()
        data = self.serializer.loads(payload)
        return set(data.get("channels", []))

    def _channel_subject(self, channel: str) -> str:
        tokenized = channel.replace(".", "_")
        return f"{self.prefix}.channel.{tokenized}"

    def _group_key(self, group: str) -> str:
        return f"group.{group}"

    def _durable_name(self, channel: str) -> str:
        return f"{self.prefix}_{channel}".replace(".", "_").replace("-", "_")

    def _ensure_open(self) -> None:
        if self._closed:
            raise ChannelLayerClosed("Channel layer has been closed")

    @staticmethod
    def _is_receive_timeout(exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        exc_type = type(exc)
        return exc_type.__module__.startswith("nats") and exc_type.__name__.endswith("TimeoutError")

    @staticmethod
    def _validate_name(kind: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{kind.title()} name must be a non-empty string")
