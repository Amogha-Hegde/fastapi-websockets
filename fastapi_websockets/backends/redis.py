from __future__ import annotations

from typing import Any, Mapping
from uuid import uuid4

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import ChannelLayerClosed, InvalidChannelLayerConfig
from fastapi_websockets.serialization import JsonSerializer


class RedisChannelLayer(BaseChannelLayer):
    """Redis-backed channel layer with queue-backed delivery and Pub/Sub notifications."""

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        prefix: str = "fastapi-websockets",
        cluster: bool = False,
        channel_expiry: int = 60,
        group_expiry: int = 86400,
        use_pubsub: bool = True,
        sharded_pubsub: bool = True,
        redis_client: Any | None = None,
        serializer: Any | None = None,
        **config: Any,
    ) -> None:
        super().__init__(
            url=url,
            prefix=prefix,
            cluster=cluster,
            channel_expiry=channel_expiry,
            group_expiry=group_expiry,
            use_pubsub=use_pubsub,
            sharded_pubsub=sharded_pubsub,
            **config,
        )
        self.url = url
        self.prefix = prefix
        self.cluster = cluster
        self.channel_expiry = channel_expiry
        self.group_expiry = group_expiry
        self.use_pubsub = use_pubsub
        self.sharded_pubsub = sharded_pubsub
        self.serializer = serializer or JsonSerializer()
        self._redis = redis_client
        self._closed = False
        self._owns_client = redis_client is None

    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("channel", channel)
        client = await self._get_client()
        payload = self.serializer.dumps(message)
        queue_key = self._channel_queue_key(channel)
        await client.rpush(queue_key, payload)
        if self.channel_expiry > 0:
            await client.expire(queue_key, self.channel_expiry)
        if self.use_pubsub:
            await self._publish_notification(client, channel, payload)

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        self._ensure_open()
        self._validate_name("channel", channel)
        client = await self._get_client()
        queue_key = self._channel_queue_key(channel)
        timeout_seconds = 0 if timeout is None else max(timeout, 0.001)
        result = await client.blpop(queue_key, timeout=timeout_seconds)
        if result is None:
            raise TimeoutError(f"Timed out waiting for channel '{channel}'")
        _, payload = result
        if self.channel_expiry > 0:
            await client.expire(queue_key, self.channel_expiry)
        return self.serializer.loads(payload)

    async def new_channel(self, prefix: str = "specific") -> str:
        self._ensure_open()
        self._validate_name("channel", prefix)
        return f"{prefix}.{uuid4().hex}"

    async def group_add(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        client = await self._get_client()
        group_key = self._group_key(group)
        await client.sadd(group_key, channel)
        if self.group_expiry > 0:
            await client.expire(group_key, self.group_expiry)

    async def group_discard(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        client = await self._get_client()
        await client.srem(self._group_key(group), channel)

    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        client = await self._get_client()
        group_key = self._group_key(group)
        channels = await client.smembers(group_key)
        for channel in channels:
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")
            await self.send(channel, message)
        if self.group_expiry > 0 and channels:
            await client.expire(group_key, self.group_expiry)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._redis is not None and self._owns_client:
            close = getattr(self._redis, "aclose", None) or getattr(self._redis, "close", None)
            if close is not None:
                result = close()
                if result is not None:
                    await result

    async def _publish_notification(self, client: Any, channel: str, payload: bytes) -> None:
        notify_channel = self._notify_channel(channel)
        if self.sharded_pubsub:
            spublish = getattr(client, "spublish", None)
            if spublish is not None:
                await spublish(notify_channel, payload)
                return
        await client.publish(notify_channel, payload)

    async def _get_client(self) -> Any:
        if self._redis is not None:
            return self._redis

        try:
            if self.cluster:
                from redis.asyncio.cluster import RedisCluster
            else:
                from redis.asyncio import Redis
        except ImportError as exc:
            raise InvalidChannelLayerConfig(
                "Redis backend requires the optional dependency group: pip install 'fastapi-websockets[redis]'"
            ) from exc

        if self.cluster:
            self._redis = RedisCluster.from_url(self.url)
        else:
            self._redis = Redis.from_url(self.url)
        return self._redis

    def _channel_queue_key(self, channel: str) -> str:
        return f"{self.prefix}:channel:{{{channel}}}:queue"

    def _notify_channel(self, channel: str) -> str:
        return f"{self.prefix}:channel:{{{channel}}}:notify"

    def _group_key(self, group: str) -> str:
        return f"{self.prefix}:group:{group}:members"

    def _ensure_open(self) -> None:
        if self._closed:
            raise ChannelLayerClosed("Channel layer has been closed")

    @staticmethod
    def _validate_name(kind: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{kind.title()} name must be a non-empty string")
