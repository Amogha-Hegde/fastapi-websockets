# fastapi-websockets

[![Tests](https://github.com/Amogha-Hegde/fastapi-websockets/actions/workflows/ci.yml/badge.svg)](https://github.com/Amogha-Hegde/fastapi-websockets/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/Amogha-Hegde/fastapi-websockets/branch/main/graph/badge.svg)](https://codecov.io/gh/Amogha-Hegde/fastapi-websockets)
[![PyPI version](https://img.shields.io/pypi/v/fastapi-websockets.svg)](https://pypi.org/project/fastapi-websockets/)
[![Python versions](https://img.shields.io/pypi/pyversions/fastapi-websockets.svg)](https://pypi.org/project/fastapi-websockets/)
[![PyPI downloads](https://static.pepy.tech/badge/fastapi-websockets)](https://pepy.tech/project/fastapi-websockets)

Channel layers and consumer primitives for FastAPI WebSocket applications.

FastAPI provides the WebSocket primitive. It does not provide a standard channel layer for multi-worker or multi-instance messaging.

`fastapi-websockets` fills that gap with a backend-agnostic API for:

- room broadcast
- per-connection messaging from other processes
- background job fan-out
- deployment across multiple workers or instances

Supported backends:

- `inmemory`
- `redis`
- `rabbitmq`
- `nats`
- `postgresql`

## Why Use It

Use this package when a WebSocket application needs to move beyond a single process.

Common requirements:

- broadcast to all connections in a room
- send to a specific connection from a worker, task queue, or separate service
- switch brokers without rewriting application code
- keep the WebSocket loop separate from broker-specific plumbing

It is possible to build a minimal version of this in application code, but the implementation usually expands quickly once group membership, backend behavior, shutdown handling, and configuration need to be shared across projects.

## Example

```python
from fastapi_websockets import get_channel_layer

layer = get_channel_layer(
    {
        "default": {
            "BACKEND": "fastapi_websockets.backends.redis.RedisChannelLayer",
            "CONFIG": {"url": "redis://localhost:6379/0"},
        }
    }
)

await layer.group_add("room:demo", websocket_channel)
await layer.group_send(
    "room:demo",
    {"type": "chat.message", "text": "hello from any worker"},
)
```

Any FastAPI worker can publish to `room:demo`. Every WebSocket subscribed to that room can receive the message. The application API stays the same if the backend changes.

## Features

- common async interface across all supported backends
- room and channel messaging primitives
- FastAPI consumer base classes
- Django-style configuration loader
- environment-based configuration support
- optional backend dependencies

## Installation

Install the core package:

```bash
pip install fastapi-websockets
```

Install backend extras as needed:

```bash
pip install "fastapi-websockets[postgresql]"
pip install "fastapi-websockets[redis]"
pip install "fastapi-websockets[nats]"
pip install "fastapi-websockets[rabbitmq]"
pip install "fastapi-websockets[test]"
```

## Configuration

Configuration follows a Django channel-layer style mapping:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "fastapi_websockets.backends.redis.RedisChannelLayer",
        "CONFIG": {
            "url": "redis://localhost:6379/0",
            "prefix": "fastapi-websockets",
            "cluster": False,
            "channel_expiry": 60,
            "group_expiry": 86400,
            "use_pubsub": True,
            "sharded_pubsub": True,
        },
    },
}
```

Build a layer from that config:

```python
from fastapi_websockets import get_channel_layer

layer = get_channel_layer(CHANNEL_LAYERS)
```

Create the layer once and reuse it for the lifetime of the application. A layer instance reuses its backend client or pool internally. Do not call `get_channel_layer()` per request or per WebSocket connection.

The layer can also be configured from environment variables:

```python
from fastapi_websockets import get_channel_layer_from_env

layer = get_channel_layer_from_env()
```

`get_channel_layer()` and `get_channel_layer_from_env()` are alias-aware. Both use the `"default"` alias unless another alias is provided:

```python
default_layer = get_channel_layer(CHANNEL_LAYERS)
events_layer = get_channel_layer(CHANNEL_LAYERS, alias="events")
```

Sample environment variables are included in `.env.sample`.

Environment variables:

- `FASTAPI_WEBSOCKETS_BACKEND`: `inmemory`, `redis`, `postgresql`, `nats`, `rabbitmq`, or a full dotted backend path
- `FASTAPI_WEBSOCKETS_INMEMORY_CAPACITY`
- `FASTAPI_WEBSOCKETS_REDIS_URL`
- `FASTAPI_WEBSOCKETS_REDIS_PREFIX`
- `FASTAPI_WEBSOCKETS_REDIS_CLUSTER`
- `FASTAPI_WEBSOCKETS_REDIS_CHANNEL_EXPIRY`
- `FASTAPI_WEBSOCKETS_REDIS_GROUP_EXPIRY`
- `FASTAPI_WEBSOCKETS_REDIS_USE_PUBSUB`
- `FASTAPI_WEBSOCKETS_REDIS_SHARDED_PUBSUB`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_DSN`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_SCHEMA`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_CHANNEL_EXPIRY`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_GROUP_EXPIRY`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_POLL_INTERVAL`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_PRUNE_INTERVAL`
- `FASTAPI_WEBSOCKETS_POSTGRESQL_ENSURE_SCHEMA`
- `FASTAPI_WEBSOCKETS_NATS_SERVERS`: comma-separated list
- `FASTAPI_WEBSOCKETS_NATS_PREFIX`
- `FASTAPI_WEBSOCKETS_NATS_GROUP_BUCKET`
- `FASTAPI_WEBSOCKETS_NATS_STREAM_NAME`
- `FASTAPI_WEBSOCKETS_NATS_MESSAGE_TIMEOUT`
- `FASTAPI_WEBSOCKETS_RABBITMQ_URL`
- `FASTAPI_WEBSOCKETS_RABBITMQ_EXCHANGE_NAME`
- `FASTAPI_WEBSOCKETS_RABBITMQ_QUEUE_PREFIX`
- `FASTAPI_WEBSOCKETS_RABBITMQ_DURABLE`
- `FASTAPI_WEBSOCKETS_RABBITMQ_MESSAGE_TTL`: integer milliseconds, or empty to disable TTL
- `FASTAPI_WEBSOCKETS_RABBITMQ_QUEUE_EXPIRY`: integer milliseconds, or empty to disable queue expiry
- `FASTAPI_WEBSOCKETS_RABBITMQ_POLL_INTERVAL`

For a single default alias, the unaliased environment variables above can be used directly.

For multiple aliases, set `FASTAPI_WEBSOCKETS_ALIASES` and prefix each alias into the variable names:

```bash
FASTAPI_WEBSOCKETS_ALIASES=default,events

FASTAPI_WEBSOCKETS_DEFAULT_BACKEND=inmemory
FASTAPI_WEBSOCKETS_DEFAULT_INMEMORY_CAPACITY=100

FASTAPI_WEBSOCKETS_EVENTS_BACKEND=postgresql
FASTAPI_WEBSOCKETS_EVENTS_POSTGRESQL_DSN=postgresql://postgres:postgres@localhost:5432/postgres
FASTAPI_WEBSOCKETS_EVENTS_POSTGRESQL_SCHEMA=fastapi_websockets_events
```

Then select the alias explicitly:

```python
events_layer = get_channel_layer_from_env(alias="events")
```

## Common API

All backends implement the same interface:

```python
await layer.send("chat.room", {"type": "message", "text": "hello"})
message = await layer.receive("chat.room")

await layer.group_add("chat-room", "chat.room")
await layer.group_send("chat-room", {"type": "broadcast", "text": "hello all"})
await layer.group_discard("chat-room", "chat.room")

channel_name = await layer.new_channel()
await layer.close()
```

Messages are mapping-like payloads. Distributed backends also preserve binary payloads inside those mappings:

```python
from fastapi_websockets import send_bytes_message

await send_bytes_message(
    layer,
    "chat.room",
    b"\x00\x01hello",
)
message = await layer.receive("chat.room")
assert message["body"] == b"\x00\x01hello"
```

Helper builders are also available when the message envelope should be constructed explicitly:

```python
from fastapi_websockets import websocket_bytes_message, websocket_json_message

await layer.send(
    "chat.room",
    websocket_bytes_message(b"\x00\x01hello", event="upload"),
)

await layer.send(
    "chat.room",
    websocket_json_message({"text": "hello"}, event="chat"),
)
```

## FastAPI WebSocket Example

This example accepts JSON and binary frames, forwards them through the channel layer, and writes them back to the client based on the message envelope:

```python
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from fastapi_websockets import (
    get_channel_layer,
    send_bytes_message,
    send_json_message,
)

app = FastAPI()
layer = get_channel_layer()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    channel_name = await layer.new_channel("ws")

    try:
        while True:
            frame = await websocket.receive()

            if frame["type"] == "websocket.disconnect":
                break

            if frame.get("bytes") is not None:
                await send_bytes_message(
                    layer,
                    channel_name,
                    frame["bytes"],
                    event="client.binary",
                )
            elif frame.get("text") is not None:
                await send_json_message(
                    layer,
                    channel_name,
                    {"text": frame["text"]},
                    event="client.text",
                )

            message = await layer.receive(channel_name)
            mode = message.get("mode")

            if mode == "bytes":
                await websocket.send_bytes(message["body"])
            elif mode == "json":
                await websocket.send_json(message["body"])
            else:
                await websocket.send_json(message)
    except WebSocketDisconnect:
        pass
```

For JSON-only input, replace `await websocket.receive()` with `await websocket.receive_json()`. For binary-only input, use `await websocket.receive_bytes()`.

For a Django Channels-style API, use the consumer classes instead of writing the WebSocket loop directly:

```python
from fastapi import FastAPI, WebSocket

from fastapi_websockets import (
    AsyncJsonWebSocketConsumer,
    get_channel_layer,
)

app = FastAPI()
layer = get_channel_layer()


class ExampleConsumer(AsyncJsonWebSocketConsumer):
    async def connect(self) -> None:
        user_id = self.path_params["user_id"]
        self.group_name = f"user_{user_id}"
        await self.group_add(self.group_name)
        await self.accept()
        await self.send_json({
            "event": "CONNECTED",
            "user_id": user_id,
        })

    async def receive_json(self, content: dict) -> None:
        response = {
            "event": "ECHO",
            "payload": content,
        }
        await self.send_json(response)

    async def send_back(self, event: dict) -> None:
        await self.send_json(event.get("data", {}))


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket) -> None:
    consumer = ExampleConsumer(layer=layer)
    await consumer(websocket)
```

Channel-layer events are dispatched by `type`, with `.` translated to `_`. For example, `{"type": "send.back", "data": {...}}` calls `send_back(event)`.

`{"type": "websocket.send", "mode": "json", "body": {...}}` and `{"type": "websocket.send", "mode": "bytes", "body": b"..."}` are handled automatically.

## In-Memory Backend

The in-memory backend is process-local. It is useful for local development, tests, and as the reference implementation for the public API.

It is not suitable for multi-process or multi-node production deployments because state is held in local memory.

## Redis Backend

The Redis backend uses Redis lists as per-channel inboxes, Redis sets for group membership, and Redis Pub/Sub notifications for fast fan-out signaling.

This keeps delivery independent of a live Pub/Sub subscription while still allowing Pub/Sub-based notifications. In practice that is safer than a pure Pub/Sub-only design when workers reconnect or restart.

Notes:

- queue keys and notification channels use Redis hash tags so related per-channel data stays slot-local
- `sharded_pubsub=True` uses `SPUBLISH` when the Redis client supports it
- group fan-out works in Redis Cluster because group membership is read from one set key and messages are then sent to each channel independently

Example:

```python
from fastapi_websockets import get_channel_layer

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "fastapi_websockets.backends.redis.RedisChannelLayer",
        "CONFIG": {
            "url": "redis://localhost:6379/0",
            "prefix": "fastapi-websockets",
            "cluster": False,
            "channel_expiry": 60,
            "group_expiry": 86400,
            "use_pubsub": True,
            "sharded_pubsub": True,
        },
    },
}

layer = get_channel_layer(CHANNEL_LAYERS)
```

## PostgreSQL Backend

The PostgreSQL backend uses regular tables for per-channel messages and group membership. Each send also emits `pg_notify`, but actual message storage stays in tables so messages survive listener reconnects and process restarts.

This backend is a better fit than pure `LISTEN/NOTIFY` when you need multi-node support without making delivery depend on PostgreSQL's small `NOTIFY` payload limit.

Notes:

- works across multiple application nodes as long as they share the same PostgreSQL database
- message delivery is table-backed, so it is not limited by `NOTIFY` payload size
- `LISTEN/NOTIFY` is used as a wake-up signal, while the tables remain the durable message store

Example:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "fastapi_websockets.backends.postgresql.PostgreSQLChannelLayer",
        "CONFIG": {
            "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
            "schema": "fastapi_websockets",
            "channel_expiry": 60,
            "group_expiry": 86400,
            "poll_interval": 0.1,
            "prune_interval": 60.0,
            "ensure_schema": True,
        },
    },
}
```

## NATS Backend

The NATS backend uses per-channel subjects for message delivery and NATS Key-Value storage for group membership. This keeps channel sends lightweight while allowing group fan-out across multiple application nodes.

Notes:

- works naturally across a NATS cluster because subjects are cluster-routed
- group membership is stored in a shared KV bucket instead of process memory
- channel delivery is JetStream-backed, so messages survive normal consumer reconnects and can be pulled by multiple app nodes

Example:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "fastapi_websockets.backends.nats.NATSChannelLayer",
        "CONFIG": {
            "servers": ["nats://localhost:4222"],
            "prefix": "fastapi-websockets",
            "group_bucket": "fastapi_websockets_groups",
            "stream_name": "FASTAPI_WEBSOCKETS",
            "message_timeout": 60.0,
        },
    },
}
```

## RabbitMQ Backend

The RabbitMQ backend uses `aio-pika`, with a direct exchange and one quorum queue per channel. Group fan-out is implemented through broker-managed bindings.

Notes:

- works across RabbitMQ clusters because queues and exchanges are broker-managed
- per-channel quorum queues provide durable delivery
- group membership is represented through queue bindings on broker-managed exchanges

Example:

```python
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "fastapi_websockets.backends.rabbitmq.RabbitMQChannelLayer",
        "CONFIG": {
            "url": "amqp://guest:guest@localhost:5672//",
            "exchange_name": "fastapi_websockets",
            "queue_prefix": "fastapi-websockets",
            "durable": True,
            "message_ttl": 60000,
            "queue_expiry": 300000,
            "poll_interval": 0.1,
        },
    },
}
```
