from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
import os
from typing import Any, Mapping, Type

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import InvalidChannelLayerConfig

DEFAULT_BACKEND = "fastapi_websockets.backends.inmemory.InMemoryChannelLayer"
DEFAULT_ENV_PREFIX = "FASTAPI_WEBSOCKETS_"
BACKEND_ALIASES = {
    "inmemory": "fastapi_websockets.backends.inmemory.InMemoryChannelLayer",
    "redis": "fastapi_websockets.backends.redis.RedisChannelLayer",
    "postgresql": "fastapi_websockets.backends.postgresql.PostgreSQLChannelLayer",
    "nats": "fastapi_websockets.backends.nats.NATSChannelLayer",
    "rabbitmq": "fastapi_websockets.backends.rabbitmq.RabbitMQChannelLayer",
}


@dataclass(frozen=True)
class BackendSettings:
    backend: str = DEFAULT_BACKEND
    config: dict[str, Any] = field(default_factory=dict)


def parse_channel_layers(
    settings: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, BackendSettings]:
    if not settings:
        return {"default": BackendSettings()}

    parsed: dict[str, BackendSettings] = {}
    for alias, value in settings.items():
        if not isinstance(value, Mapping):
            raise InvalidChannelLayerConfig(
                f"Layer '{alias}' must be a mapping with BACKEND and CONFIG keys"
            )
        backend = value.get("BACKEND", DEFAULT_BACKEND)
        config = value.get("CONFIG", {})
        if not isinstance(config, Mapping):
            raise InvalidChannelLayerConfig(
                f"Layer '{alias}' CONFIG must be a mapping"
            )
        parsed[alias] = BackendSettings(backend=backend, config=dict(config))
    return parsed


def load_backend_class(path: str) -> Type[BaseChannelLayer]:
    if "." not in path:
        raise InvalidChannelLayerConfig(
            f"Backend path '{path}' must be a full dotted import path"
        )

    module_path, class_name = path.rsplit(".", 1)
    try:
        module = import_module(module_path)
    except ImportError as exc:
        raise InvalidChannelLayerConfig(
            f"Could not import backend module '{module_path}'"
        ) from exc

    try:
        backend_class = getattr(module, class_name)
    except AttributeError as exc:
        raise InvalidChannelLayerConfig(
            f"Backend class '{class_name}' was not found in '{module_path}'"
        ) from exc

    if not issubclass(backend_class, BaseChannelLayer):
        raise InvalidChannelLayerConfig(
            f"Backend '{path}' must inherit from BaseChannelLayer"
        )
    return backend_class


def build_channel_layer(backend: str, config: Mapping[str, Any] | None = None) -> BaseChannelLayer:
    backend_class = load_backend_class(backend)
    return backend_class(**dict(config or {}))


def parse_channel_layers_from_env(
    environ: Mapping[str, str] | None = None,
    prefix: str = DEFAULT_ENV_PREFIX,
    alias: str = "default",
) -> dict[str, BackendSettings]:
    env = environ or os.environ
    backend_value = env.get(f"{prefix}BACKEND", "inmemory").strip()
    backend = _resolve_backend_path(backend_value)
    config = _parse_backend_env_config(backend, env, prefix)
    return {alias: BackendSettings(backend=backend, config=config)}


def get_channel_layer_from_env(
    environ: Mapping[str, str] | None = None,
    prefix: str = DEFAULT_ENV_PREFIX,
    alias: str = "default",
) -> BaseChannelLayer:
    settings = parse_channel_layers_from_env(environ=environ, prefix=prefix, alias=alias)
    return get_channel_layer(
        {
            name: {"BACKEND": value.backend, "CONFIG": value.config}
            for name, value in settings.items()
        },
        alias=alias,
    )


def get_channel_layer(
    settings: Mapping[str, Mapping[str, Any]] | None = None,
    alias: str = "default",
) -> BaseChannelLayer:
    layers = parse_channel_layers(settings)
    if alias not in layers:
        raise InvalidChannelLayerConfig(f"Channel layer alias '{alias}' is not defined")
    layer_settings = layers[alias]
    return build_channel_layer(layer_settings.backend, layer_settings.config)


def _resolve_backend_path(value: str) -> str:
    if not value:
        return DEFAULT_BACKEND
    return BACKEND_ALIASES.get(value.lower(), value)


def _parse_backend_env_config(
    backend: str, environ: Mapping[str, str], prefix: str
) -> dict[str, Any]:
    if backend == BACKEND_ALIASES["inmemory"]:
        return {
            "capacity": _get_int(environ, f"{prefix}INMEMORY_CAPACITY", 100),
        }
    if backend == BACKEND_ALIASES["redis"]:
        return {
            "url": environ.get(f"{prefix}REDIS_URL", "redis://localhost:6379/0"),
            "prefix": environ.get(f"{prefix}REDIS_PREFIX", "fastapi-websockets"),
            "cluster": _get_bool(environ, f"{prefix}REDIS_CLUSTER", False),
            "channel_expiry": _get_int(environ, f"{prefix}REDIS_CHANNEL_EXPIRY", 60),
            "group_expiry": _get_int(environ, f"{prefix}REDIS_GROUP_EXPIRY", 86400),
            "use_pubsub": _get_bool(environ, f"{prefix}REDIS_USE_PUBSUB", True),
            "sharded_pubsub": _get_bool(
                environ, f"{prefix}REDIS_SHARDED_PUBSUB", True
            ),
        }
    if backend == BACKEND_ALIASES["postgresql"]:
        return {
            "dsn": environ.get(
                f"{prefix}POSTGRESQL_DSN",
                "postgresql://postgres:postgres@localhost:5432/postgres",
            ),
            "schema": environ.get(f"{prefix}POSTGRESQL_SCHEMA", "fastapi_websockets"),
            "channel_expiry": _get_int(
                environ, f"{prefix}POSTGRESQL_CHANNEL_EXPIRY", 60
            ),
            "group_expiry": _get_int(
                environ, f"{prefix}POSTGRESQL_GROUP_EXPIRY", 86400
            ),
            "poll_interval": _get_float(
                environ, f"{prefix}POSTGRESQL_POLL_INTERVAL", 0.1
            ),
            "ensure_schema": _get_bool(
                environ, f"{prefix}POSTGRESQL_ENSURE_SCHEMA", True
            ),
        }
    if backend == BACKEND_ALIASES["nats"]:
        return {
            "servers": _get_csv_list(
                environ, f"{prefix}NATS_SERVERS", ["nats://localhost:4222"]
            ),
            "prefix": environ.get(f"{prefix}NATS_PREFIX", "fastapi-websockets"),
            "group_bucket": environ.get(
                f"{prefix}NATS_GROUP_BUCKET", "fastapi_websockets_groups"
            ),
            "stream_name": environ.get(
                f"{prefix}NATS_STREAM_NAME", "FASTAPI_WEBSOCKETS"
            ),
            "message_timeout": _get_float(
                environ, f"{prefix}NATS_MESSAGE_TIMEOUT", 60.0
            ),
        }
    if backend == BACKEND_ALIASES["rabbitmq"]:
        return {
            "url": environ.get(
                f"{prefix}RABBITMQ_URL", "amqp://guest:guest@localhost:5672//"
            ),
            "exchange_name": environ.get(
                f"{prefix}RABBITMQ_EXCHANGE_NAME", "fastapi_websockets"
            ),
            "queue_prefix": environ.get(
                f"{prefix}RABBITMQ_QUEUE_PREFIX", "fastapi-websockets"
            ),
            "durable": _get_bool(environ, f"{prefix}RABBITMQ_DURABLE", True),
            "message_ttl": _get_optional_int(
                environ, f"{prefix}RABBITMQ_MESSAGE_TTL", 60000
            ),
        }
    return {}


def _get_bool(environ: Mapping[str, str], key: str, default: bool) -> bool:
    value = environ.get(key)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise InvalidChannelLayerConfig(f"Environment variable '{key}' must be a boolean")


def _get_int(environ: Mapping[str, str], key: str, default: int) -> int:
    value = environ.get(key)
    if value is None:
        return default
    try:
        return int(value.strip())
    except ValueError as exc:
        raise InvalidChannelLayerConfig(
            f"Environment variable '{key}' must be an integer"
        ) from exc


def _get_optional_int(
    environ: Mapping[str, str], key: str, default: int | None
) -> int | None:
    value = environ.get(key)
    if value is None:
        return default
    if not value.strip():
        return None
    try:
        return int(value.strip())
    except ValueError as exc:
        raise InvalidChannelLayerConfig(
            f"Environment variable '{key}' must be an integer or empty"
        ) from exc


def _get_float(environ: Mapping[str, str], key: str, default: float) -> float:
    value = environ.get(key)
    if value is None:
        return default
    try:
        return float(value.strip())
    except ValueError as exc:
        raise InvalidChannelLayerConfig(
            f"Environment variable '{key}' must be a float"
        ) from exc


def _get_csv_list(
    environ: Mapping[str, str], key: str, default: list[str]
) -> list[str]:
    value = environ.get(key)
    if value is None:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or default
