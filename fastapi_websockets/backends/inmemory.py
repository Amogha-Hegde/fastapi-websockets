from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Mapping
from uuid import uuid4

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import ChannelFull, ChannelLayerClosed


class InMemoryChannelLayer(BaseChannelLayer):
    """Process-local reference implementation for development and testing."""

    def __init__(self, capacity: int = 100, **config: Any) -> None:
        super().__init__(capacity=capacity, **config)
        self.capacity = capacity
        self._channels: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._groups: defaultdict[str, set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._closed = False

    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        queue = await self._get_or_create_queue(channel)
        try:
            queue.put_nowait(dict(message))
        except asyncio.QueueFull as exc:
            raise ChannelFull(f"Channel '{channel}' is full") from exc

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        self._ensure_open()
        queue = await self._get_or_create_queue(channel)
        if timeout is None:
            return await queue.get()
        return await asyncio.wait_for(queue.get(), timeout=timeout)

    async def new_channel(self, prefix: str = "specific") -> str:
        self._ensure_open()
        channel = f"{prefix}.{uuid4().hex}"
        await self._get_or_create_queue(channel)
        return channel

    async def group_add(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        await self._get_or_create_queue(channel)
        async with self._lock:
            self._groups[group].add(channel)

    async def group_discard(self, group: str, channel: str) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        self._validate_name("channel", channel)
        async with self._lock:
            channels = self._groups.get(group)
            if not channels:
                return
            channels.discard(channel)
            if not channels:
                self._groups.pop(group, None)

    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        self._ensure_open()
        self._validate_name("group", group)
        async with self._lock:
            targets = tuple(self._groups.get(group, ()))
        for channel in targets:
            await self.send(channel, message)

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            self._channels.clear()
            self._groups.clear()

    async def _get_or_create_queue(self, channel: str) -> asyncio.Queue[dict[str, Any]]:
        self._validate_name("channel", channel)
        async with self._lock:
            queue = self._channels.get(channel)
            if queue is None:
                queue = asyncio.Queue(maxsize=self.capacity)
                self._channels[channel] = queue
            return queue

    def _ensure_open(self) -> None:
        if self._closed:
            raise ChannelLayerClosed("Channel layer has been closed")

    @staticmethod
    def _validate_name(kind: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{kind.title()} name must be a non-empty string")
