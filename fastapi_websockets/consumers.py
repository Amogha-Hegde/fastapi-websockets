from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.config import get_channel_layer
from fastapi_websockets.exceptions import ChannelLayerClosed


class AsyncWebSocketConsumer:
    """Channels-style async websocket consumer for FastAPI."""

    channel_name_prefix = "specific"

    def __init__(self, layer: BaseChannelLayer | None = None) -> None:
        self.channel_layer = layer or get_channel_layer()
        self.websocket: WebSocket | None = None
        self.scope: dict[str, Any] = {}
        self.channel_name = ""
        self._accepted = False
        self._closed = False
        self._joined_groups: set[str] = set()

    async def __call__(self, websocket: WebSocket) -> None:
        self.websocket = websocket
        self.scope = dict(websocket.scope)
        self.channel_name = await self.channel_layer.new_channel(self.channel_name_prefix)
        close_code: int | None = None
        events: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        websocket_pump: asyncio.Task[Any] | None = None
        channel_pump: asyncio.Task[Any] | None = None

        try:
            await self.connect()
            if not self._accepted and not self._closed:
                await self.accept()

            websocket_pump = asyncio.create_task(self._pump_websocket(events))
            channel_pump = asyncio.create_task(self._pump_channel(events))

            while not self._closed:
                source, payload = await events.get()
                if source == "websocket":
                    frame = payload
                    if frame["type"] == "websocket.disconnect":
                        close_code = frame.get("code")
                        await self._drain_pending_channel_events(events)
                        break
                    await self.dispatch_websocket_receive(frame)
                    continue
                await self.dispatch_channel_message(payload)
        except WebSocketDisconnect as exc:
            close_code = exc.code
        finally:
            for task in (websocket_pump, channel_pump):
                if task is not None:
                    task.cancel()
            for task in (websocket_pump, channel_pump):
                if task is not None:
                    with contextlib.suppress(asyncio.CancelledError, ChannelLayerClosed):
                        await task
            with contextlib.suppress(ChannelLayerClosed):
                await self._cleanup_groups()
            with contextlib.suppress(ChannelLayerClosed):
                await self.disconnect(close_code)

    async def connect(self) -> None:
        await self.accept()

    async def disconnect(self, close_code: int | None) -> None:
        del close_code

    async def receive(
        self,
        text_data: str | None = None,
        bytes_data: bytes | None = None,
    ) -> None:
        if text_data is not None:
            await self.receive_text(text_data)
            return
        if bytes_data is not None:
            await self.receive_bytes(bytes_data)

    async def receive_text(self, text_data: str) -> None:
        del text_data

    async def receive_bytes(self, bytes_data: bytes) -> None:
        del bytes_data

    async def channel_receive(self, message: dict[str, Any]) -> None:
        if message.get("type") == "websocket.send":
            await self.websocket_send(message)

    async def websocket_send(self, message: dict[str, Any]) -> None:
        mode = message.get("mode")
        body = message.get("body")

        if mode == "bytes":
            await self.send_bytes(body)
            return
        if mode == "json":
            await self.send_json(body)
            return
        if isinstance(body, (bytes, bytearray, memoryview)):
            await self.send_bytes(bytes(body))
            return
        if isinstance(body, str):
            await self.send_text(body)
            return
        await self.send_json(body if body is not None else message)

    async def accept(
        self,
        subprotocol: str | None = None,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        websocket = self._require_websocket()
        await websocket.accept(subprotocol=subprotocol, headers=headers)
        self._accepted = True

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        websocket = self._require_websocket()
        await websocket.close(code=code, reason=reason)
        self._closed = True

    async def send_text(self, text: str) -> None:
        websocket = self._require_websocket()
        await websocket.send_text(text)

    async def send_bytes(self, data: bytes | bytearray | memoryview) -> None:
        websocket = self._require_websocket()
        if not isinstance(data, bytes):
            data = bytes(data)
        await websocket.send_bytes(data)

    async def send_json(self, data: Any) -> None:
        websocket = self._require_websocket()
        await websocket.send_json(data)

    async def group_add(self, group: str) -> None:
        await self.channel_layer.group_add(group, self.channel_name)
        self._joined_groups.add(group)

    async def group_discard(self, group: str) -> None:
        await self.channel_layer.group_discard(group, self.channel_name)
        self._joined_groups.discard(group)

    async def dispatch_websocket_receive(self, frame: dict[str, Any]) -> None:
        if frame.get("text") is not None:
            await self.receive(text_data=frame["text"])
            return
        if frame.get("bytes") is not None:
            await self.receive(bytes_data=frame["bytes"])
            return
        await self.receive()

    async def dispatch_channel_message(self, message: Any) -> None:
        if not isinstance(message, dict):
            return
        event_type = message.get("type")
        if not isinstance(event_type, str) or not event_type.strip():
            return
        handler = getattr(self, event_type.replace(".", "_"), None)
        if handler is not None:
            await handler(message)
            return
        await self.channel_receive(message)

    @property
    def path_params(self) -> dict[str, Any]:
        websocket = self._require_websocket()
        return dict(websocket.path_params)

    @property
    def query_params(self) -> dict[str, str]:
        websocket = self._require_websocket()
        return dict(websocket.query_params)

    async def _pump_websocket(self, events: asyncio.Queue[tuple[str, Any]]) -> None:
        websocket = self._require_websocket()
        while True:
            frame = await websocket.receive()
            await events.put(("websocket", frame))
            if frame["type"] == "websocket.disconnect":
                return

    async def _pump_channel(self, events: asyncio.Queue[tuple[str, Any]]) -> None:
        while True:
            message = await self.channel_layer.receive(self.channel_name)
            await events.put(("channel", message))

    async def _cleanup_groups(self) -> None:
        for group in tuple(self._joined_groups):
            await self.channel_layer.group_discard(group, self.channel_name)
            self._joined_groups.discard(group)

    async def _drain_pending_channel_events(
        self,
        events: asyncio.Queue[tuple[str, Any]],
    ) -> None:
        await asyncio.sleep(0)
        while not events.empty():
            source, payload = events.get_nowait()
            if source == "channel":
                await self.dispatch_channel_message(payload)

    def _require_websocket(self) -> WebSocket:
        if self.websocket is None:
            raise RuntimeError("WebSocket consumer is not bound to a websocket")
        return self.websocket


class AsyncJsonWebSocketConsumer(AsyncWebSocketConsumer):
    """Channels-style consumer that parses JSON text frames."""

    async def receive(
        self,
        text_data: str | None = None,
        bytes_data: bytes | None = None,
    ) -> None:
        if bytes_data is not None:
            await self.receive_bytes(bytes_data)
            return
        if text_data is None:
            return
        content = json.loads(text_data)
        await self.receive_json(content)

    async def receive_json(self, content: Any) -> None:
        del content
