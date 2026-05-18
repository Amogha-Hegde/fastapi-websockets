# fastapi-websockets

Backend-agnostic channel layers for FastAPI WebSocket workloads.

The package is modeled after the channel-layer style used in Django: you configure one backend behind a common async interface and swap transports without changing application code.

## Status

Implemented now:

- Core channel layer interface
- Django-style backend configuration loader
- `inmemory` backend
- `nats` backend
- `postgresql` backend
- `rabbitmq` backend
- `redis` backend

Planned next:

- backend contract refinements
- integration-test coverage with real services

## Goals

- One common API across all backends
- Async-first interface for FastAPI applications
- Support both single-node and clustered deployments
- Keep backend dependencies optional

## Installation

Core package:

```bash
pip install fastapi-websockets
```

Backend extras:

```bash
pip install "fastapi-websockets[postgresql]"
pip install "fastapi-websockets[redis]"
pip install "fastapi-websockets[nats]"
pip install "fastapi-websockets[rabbitmq]"
pip install "fastapi-websockets[test]"
```

## Agent Skill

The package includes a repo/library skill at `fastapi_websockets/.agents/skills/fastapi-websockets/SKILL.md`.

Use that skill when:

- changing code in this repository
- writing or modifying application code that integrates with `fastapi-websockets`

The skill is packaged with the library so agent-aware tools can discover the same guidance from an installed copy of `fastapi-websockets`, not only from this repository checkout.

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

You can build a layer from that config:

```python
from fastapi_websockets import get_channel_layer

layer = get_channel_layer(CHANNEL_LAYERS)
```

Create the layer once and reuse it for the lifetime of the app. A layer instance reuses its backend client or pool internally; do not call `get_channel_layer()` per request or per websocket connection.

You can also configure the layer from environment variables:

```python
from fastapi_websockets import get_channel_layer_from_env

layer = get_channel_layer_from_env()
```

`get_channel_layer()` and `get_channel_layer_from_env()` are alias-aware. Both default to the `"default"` alias:

```python
default_layer = get_channel_layer(CHANNEL_LAYERS)
events_layer = get_channel_layer(CHANNEL_LAYERS, alias="events")
```

The package includes a sample env file at `.env.sample`.

Environment variable contract:

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
- `FASTAPI_WEBSOCKETS_RABBITMQ_POLL_INTERVAL`

For a single default alias, the unaliased env vars above still work.

For multiple aliases, set `FASTAPI_WEBSOCKETS_ALIASES` and prefix each alias into the variable names:

```bash
FASTAPI_WEBSOCKETS_ALIASES=default,events

FASTAPI_WEBSOCKETS_DEFAULT_BACKEND=inmemory
FASTAPI_WEBSOCKETS_DEFAULT_INMEMORY_CAPACITY=100

FASTAPI_WEBSOCKETS_EVENTS_BACKEND=postgresql
FASTAPI_WEBSOCKETS_EVENTS_POSTGRESQL_DSN=postgresql://postgres:postgres@localhost:5432/postgres
FASTAPI_WEBSOCKETS_EVENTS_POSTGRESQL_SCHEMA=fastapi_websockets_events
```

Then select the alias you want:

```python
events_layer = get_channel_layer_from_env(alias="events")
```

## Common API

All backends are expected to support this interface:

```python
await layer.send("chat.room", {"type": "message", "text": "hello"})
message = await layer.receive("chat.room")

await layer.group_add("chat-room", "chat.room")
await layer.group_send("chat-room", {"type": "broadcast", "text": "hello all"})
await layer.group_discard("chat-room", "chat.room")

channel_name = await layer.new_channel()
await layer.close()
```

Messages are JSON-style mappings, but distributed backends also preserve binary payloads inside those mappings. For example:

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

If you want to build the envelope explicitly, helper builders are also available:

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

## FastAPI WebSocket example

Here is a minimal FastAPI endpoint that accepts both JSON and binary frames, forwards them through the channel layer, and writes them back to the client based on the message envelope:

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

If you only want JSON input, you can replace `await websocket.receive()` with `await websocket.receive_json()`. If you only want binary input, use `await websocket.receive_bytes()`.

If you want a Django Channels-style API, use the consumer classes instead of writing the websocket loop yourself:

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

Channel-layer events are dispatched by `type`, with `.` translated to `_`. For example, `{"type": "send.back", "data": {...}}` will call `send_back(event)`.

`{"type": "websocket.send", "mode": "json", "body": {...}}` and `{"type": "websocket.send", "mode": "bytes", "body": b"..."}` are handled automatically.

## In-memory backend

The in-memory backend is process-local. It is useful for local development, tests, and as the reference implementation for the public API.

It is not suitable for multi-process or multi-node production deployments because state is held in local memory.

## Redis backend

The Redis backend uses Redis lists as per-channel inboxes, Redis sets for group membership, and Redis Pub/Sub notifications for fast fan-out signaling.

This keeps delivery independent of a live Pub/Sub subscription while still allowing Pub/Sub-based notifications. In practice that is safer than a pure Pub/Sub-only design when workers reconnect or restart.

Cluster notes:

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

## PostgreSQL backend

The PostgreSQL backend uses regular tables for per-channel messages and group membership. Each send also emits `pg_notify`, but actual message storage stays in tables so messages survive listener reconnects and process restarts.

This backend is a better fit than pure `LISTEN/NOTIFY` when you need multi-node support without making delivery depend on PostgreSQL's small `NOTIFY` payload limit.

Cluster notes:

- works across multiple application nodes as long as they share the same PostgreSQL database
- message delivery is table-backed, so it is not limited by `NOTIFY` payload size
- current receive behavior is polling-based over stored messages; `pg_notify` is emitted for future push-style wakeups

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
            "ensure_schema": True,
        },
    },
}
```

## NATS backend

The NATS backend uses per-channel subjects for message delivery and NATS Key-Value storage for group membership. This keeps channel sends lightweight while allowing group fan-out across multiple application nodes.

Cluster notes:

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

## RabbitMQ backend

The RabbitMQ backend now uses `aio-pika`, with a direct exchange plus one queue per channel. Group fan-out is implemented by resolving group members and publishing one message per target channel.

Cluster notes:

- works across RabbitMQ clusters because queues and exchanges are broker-managed
- per-channel queues provide durable delivery when `durable=True`
- current group membership is held in process memory, so this first pass is suitable for single-node app membership management but not yet for fully shared multi-node group state

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
            "poll_interval": 0.1,
        },
    },
}
```
