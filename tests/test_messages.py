import asyncio

from fastapi_websockets import (
    InMemoryChannelLayer,
    send_bytes_message,
    send_json_message,
    websocket_bytes_message,
    websocket_json_message,
)


def test_websocket_bytes_message_builds_expected_envelope() -> None:
    message = websocket_bytes_message(
        b"\x00\x01hello",
        room="alpha",
    )
    assert message == {
        "type": "websocket.send",
        "mode": "bytes",
        "body": b"\x00\x01hello",
        "room": "alpha",
    }


def test_websocket_json_message_builds_expected_envelope() -> None:
    message = websocket_json_message(
        {"text": "hello"},
        room="alpha",
    )
    assert message == {
        "type": "websocket.send",
        "mode": "json",
        "body": {"text": "hello"},
        "room": "alpha",
    }


def test_websocket_bytes_message_coerces_bytearray_and_memoryview() -> None:
    bytearray_message = websocket_bytes_message(bytearray(b"abc"))
    memoryview_message = websocket_bytes_message(memoryview(b"xyz"))
    assert bytearray_message["body"] == b"abc"
    assert memoryview_message["body"] == b"xyz"


def test_send_bytes_message_uses_envelope() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await send_bytes_message(layer, "chat.room", b"\x00\x01hello", event="upload")
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == {
            "type": "websocket.send",
            "mode": "bytes",
            "body": b"\x00\x01hello",
            "event": "upload",
        }

    asyncio.run(run())


def test_send_json_message_uses_envelope() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await send_json_message(layer, "chat.room", {"text": "hello"}, event="chat")
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == {
            "type": "websocket.send",
            "mode": "json",
            "body": {"text": "hello"},
            "event": "chat",
        }

    asyncio.run(run())
