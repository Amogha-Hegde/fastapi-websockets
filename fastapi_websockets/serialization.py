from __future__ import annotations

import base64
import json
from typing import Any, Mapping


class JsonSerializer:
    """Default serializer boundary shared by distributed backends."""

    _TYPE_KEY = "__fastapi_websockets_type__"
    _BYTES_TYPE = "bytes"

    def dumps(self, message: Mapping[str, Any]) -> bytes:
        normalized = self._normalize(message)
        return json.dumps(normalized, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def loads(self, payload: bytes | str) -> dict[str, Any]:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return self._restore(json.loads(payload))

    def _normalize(self, value: Any) -> Any:
        if isinstance(value, bytes):
            return {
                self._TYPE_KEY: self._BYTES_TYPE,
                "data": base64.b64encode(value).decode("ascii"),
            }
        if isinstance(value, bytearray):
            return self._normalize(bytes(value))
        if isinstance(value, memoryview):
            return self._normalize(value.tobytes())
        if isinstance(value, Mapping):
            return {str(key): self._normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize(item) for item in value]
        if isinstance(value, tuple):
            return [self._normalize(item) for item in value]
        return value

    def _restore(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._restore(item) for item in value]
        if isinstance(value, dict):
            tag = value.get(self._TYPE_KEY)
            if tag == self._BYTES_TYPE:
                encoded = value.get("data", "")
                return base64.b64decode(encoded.encode("ascii"))
            return {key: self._restore(item) for key, item in value.items()}
        return value
