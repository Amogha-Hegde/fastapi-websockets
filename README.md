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
        },
    },
}
```

## Next steps

Each distributed backend will continue to document:

- topology assumptions
- delivery guarantees
- clustering behavior
- operational caveats
