---
name: fastapi-websockets
description: Use when modifying this repository. Covers the intended architecture for channel layers, consumers, message envelopes, env aliasing, lazy backend clients, and the repo's do/don't rules for changes and tests.
---

# fastapi-websockets

Use this skill when changing code in this repository.

## Purpose

This package provides:

- backend-agnostic channel layers for FastAPI websocket workloads
- optional distributed backends with lazy client creation
- message helpers for websocket-style JSON and bytes envelopes
- Channels-style async consumers on top of FastAPI `WebSocket`

This package does not aim to hide backend-specific delivery guarantees. Keep the public API consistent while documenting backend differences clearly.

## Core Architecture

Keep these layers separate:

1. `backends/`
   Implements the `BaseChannelLayer` contract only.
2. `config.py`
   Builds layers from Django-style settings and env vars, including alias support.
3. `messages.py`
   Provides convenience envelope builders and send helpers.
4. `consumers.py`
   Provides higher-level websocket consumer abstractions for FastAPI.
5. `serialization.py`
   Defines the serializer boundary for distributed backends.

Do not collapse these responsibilities together.

## Public Contracts

### Channel layer contract

Backends must preserve the `BaseChannelLayer` API:

- `send`
- `receive`
- `new_channel`
- `group_add`
- `group_discard`
- `group_send`
- `close`

Keep the contract async-first and backend-agnostic.

### Message contract

Messages are mappings.

Websocket-oriented envelopes use:

```python
{"type": "websocket.send", "mode": "json", "body": {...}}
{"type": "websocket.send", "mode": "bytes", "body": b"..."}
```

For custom consumer events, `type` is dispatched by replacing `.` with `_`.

Example:

```python
{"type": "send.back", "data": {...}}
```

dispatches to:

```python
async def send_back(self, event: dict) -> None:
    ...
```

### Config contract

`get_channel_layer()` and `get_channel_layer_from_env()` default to the `"default"` alias.

Environment config must support both:

- legacy single-alias env vars like `FASTAPI_WEBSOCKETS_BACKEND`
- multi-alias env vars via `FASTAPI_WEBSOCKETS_ALIASES` and alias-prefixed keys

Backward compatibility for the unaliased `"default"` path matters.

## Backend Rules

### Do

- lazily import optional backend dependencies inside backend connection builders
- lazily create the backend client or pool on first real use
- cache and reuse the created client/pool on the layer instance
- close only internally owned clients in `close()`
- keep tests injection-friendly by allowing fake clients/pools to be passed in
- preserve bytes payloads through distributed backends via the serializer boundary

### Do not

- import `redis`, `asyncpg`, `nats`, or `aio_pika` at module import time
- create a new backend client on every `send`, `receive`, or `group_*` call
- require applications to pass backend connection objects for normal usage
- move backend-specific client creation into application examples
- silently change backend semantics without updating docs and tests

## Consumer Rules

### Do

- keep `AsyncWebSocketConsumer` generic
- keep `AsyncJsonWebSocketConsumer` as a convenience layer for JSON text frames
- preserve FastAPI `WebSocket` compatibility
- maintain helper methods such as `accept`, `close`, `send_text`, `send_json`, `send_bytes`, `group_add`, and `group_discard`
- keep event dispatch deterministic and easy to follow
- ensure disconnect cleanup discards joined groups

### Do not

- hardcode app-specific consumer names or business logic into the library
- make consumer examples domain-specific when a neutral example will work
- force JSON-only behavior in the generic consumer base
- mix business validation rules into the core consumer classes

## Docs Rules

### Do

- keep README examples generic, reusable, and framework-local
- show `layer = get_channel_layer(...)` or `get_channel_layer_from_env()` at module scope
- state clearly that one layer instance should be reused across app lifetime
- document backend caveats when behavior differs across implementations
- update `.env.sample` when env behavior changes

### Do not

- show per-request or per-websocket calls to `get_channel_layer()`
- imply that callers must manually create backend clients in normal usage
- use business-specific names like `TellerConsumer` in public examples
- add docs that contradict actual lazy-connection behavior

## Testing Rules

Run targeted tests for changed areas and prefer the full suite before finishing.

### Do

- add or update tests for every public behavior change
- keep fake backend clients lightweight and deterministic
- test both backward compatibility and the new path when changing config/env behavior
- verify consumer behavior for websocket frames, channel events, and disconnect cleanup
- run `pytest -q`

### Do not

- change public behavior without tests
- rely on real external services in unit tests
- weaken assertions unless the behavior is intentionally concurrent or nondeterministic

## Change Patterns

### Adding a new backend option

- extend config parsing
- keep dependency optional
- add backend-specific tests
- document config in README and `.env.sample`

### Adding a new message helper

- keep it in `messages.py`
- export it from `fastapi_websockets.__init__`
- add tests for the helper and the resulting envelope

### Changing env parsing

- preserve `"default"` alias behavior
- test both single-alias and multi-alias env layouts
- update README and `.env.sample`

### Changing consumer behavior

- test websocket input, channel input, disconnect behavior, and helper dispatch
- keep neutral example names in docs

## Review Checklist

Before finishing, verify:

- public API shape is still coherent
- optional dependencies are still lazy
- one layer instance still reuses one backend client/pool
- docs match runtime behavior
- tests cover the changed contract
