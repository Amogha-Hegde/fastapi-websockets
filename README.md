# fastapi-websockets

Backend-agnostic channel layers for FastAPI WebSocket workloads.

The package is modeled after the channel-layer style used in Django: you configure one backend behind a common async interface and swap transports without changing application code.

## Status

Implemented now:

- Core channel layer interface
- Django-style backend configuration loader
- `inmemory` backend

Planned next:

- `postgresql`
- `redis` using Redis Pub/Sub with cluster-safe design
- `nats`
- `rabbitmq`

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
        "BACKEND": "fastapi_websockets.backends.inmemory.InMemoryChannelLayer",
        "CONFIG": {
            "capacity": 100,
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

## Next steps

The next backend to implement is Redis, followed by PostgreSQL, NATS, and RabbitMQ. Each distributed backend will document:

- topology assumptions
- delivery guarantees
- clustering behavior
- operational caveats
