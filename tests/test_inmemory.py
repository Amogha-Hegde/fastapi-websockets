import asyncio

from fastapi_websockets.backends.inmemory import InMemoryChannelLayer
from fastapi_websockets.exceptions import ChannelFull, ChannelLayerClosed


def test_send_and_receive_round_trip() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await layer.send("chat.room", {"type": "message", "text": "hello"})
        message = await layer.receive("chat.room", timeout=0.1)
        assert message == {"type": "message", "text": "hello"}

    asyncio.run(run())


def test_group_send_fans_out_to_all_members() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await layer.group_add("room", "channel.one")
        await layer.group_add("room", "channel.two")

        await layer.group_send("room", {"type": "broadcast", "text": "hi"})

        first = await layer.receive("channel.one", timeout=0.1)
        second = await layer.receive("channel.two", timeout=0.1)
        assert first == {"type": "broadcast", "text": "hi"}
        assert second == {"type": "broadcast", "text": "hi"}

    asyncio.run(run())


def test_group_discard_stops_future_delivery() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await layer.group_add("room", "channel.one")
        await layer.group_discard("room", "channel.one")

        await layer.group_send("room", {"type": "broadcast"})
        try:
            await layer.receive("channel.one", timeout=0.01)
        except TimeoutError:
            pass
        else:
            raise AssertionError("Expected no message after group_discard")

    asyncio.run(run())


def test_new_channel_creates_unique_names() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        first = await layer.new_channel()
        second = await layer.new_channel()
        assert first != second
        assert first.startswith("specific.")
        assert second.startswith("specific.")

    asyncio.run(run())


def test_capacity_limit_raises_channel_full() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer(capacity=1)
        await layer.send("chat.room", {"sequence": 1})
        try:
            await layer.send("chat.room", {"sequence": 2})
        except ChannelFull:
            pass
        else:
            raise AssertionError("Expected ChannelFull when queue capacity is exceeded")

    asyncio.run(run())


def test_closed_layer_rejects_new_operations() -> None:
    async def run() -> None:
        layer = InMemoryChannelLayer()
        await layer.close()
        try:
            await layer.send("chat.room", {"type": "message"})
        except ChannelLayerClosed:
            pass
        else:
            raise AssertionError("Expected ChannelLayerClosed after close")

    asyncio.run(run())
