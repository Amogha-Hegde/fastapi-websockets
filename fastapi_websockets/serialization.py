from __future__ import annotations

import json
from typing import Any, Mapping


class JsonSerializer:
    """Default serializer boundary shared by distributed backends."""

    def dumps(self, message: Mapping[str, Any]) -> bytes:
        return json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def loads(self, payload: bytes | str) -> dict[str, Any]:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(payload)
