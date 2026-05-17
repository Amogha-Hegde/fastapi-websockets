from __future__ import annotations

from typing import Any, Mapping

from fastapi_websockets.backends.base import BaseChannelLayer


def websocket_bytes_message(
    body: bytes | bytearray | memoryview,
    *,
    message_type: str = "websocket.send",
    **extra: Any,
) -> dict[str, Any]:
    return _build_websocket_message(
        mode="bytes",
        body=_coerce_bytes(body),
        message_type=message_type,
        extra=extra,
    )


def websocket_json_message(
    body: Any,
    *,
    message_type: str = "websocket.send",
    **extra: Any,
) -> dict[str, Any]:
    return _build_websocket_message(
        mode="json",
        body=body,
        message_type=message_type,
        extra=extra,
    )


async def send_bytes_message(
    layer: BaseChannelLayer,
    channel: str,
    body: bytes | bytearray | memoryview,
    *,
    message_type: str = "websocket.send",
    **extra: Any,
) -> None:
    await layer.send(
        channel,
        websocket_bytes_message(body, message_type=message_type, **extra),
    )


async def send_json_message(
    layer: BaseChannelLayer,
    channel: str,
    body: Any,
    *,
    message_type: str = "websocket.send",
    **extra: Any,
) -> None:
    await layer.send(
        channel,
        websocket_json_message(body, message_type=message_type, **extra),
    )


def _build_websocket_message(
    *,
    mode: str,
    body: Any,
    message_type: str,
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    message = dict(extra)
    message["type"] = message_type
    message["mode"] = mode
    message["body"] = body
    return message


def _coerce_bytes(value: bytes | bytearray | memoryview) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise TypeError("Binary websocket body must be bytes, bytearray, or memoryview")
