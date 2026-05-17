from fastapi_websockets.backends.inmemory import InMemoryChannelLayer
from fastapi_websockets.config import (
    BACKEND_ALIASES,
    DEFAULT_BACKEND,
    build_channel_layer,
    get_channel_layer,
    get_channel_layer_from_env,
    load_backend_class,
    parse_channel_layers,
    parse_channel_layers_from_env,
)
from fastapi_websockets.exceptions import InvalidChannelLayerConfig


def test_parse_channel_layers_uses_default_config_when_none() -> None:
    layers = parse_channel_layers(None)
    assert layers["default"].backend == DEFAULT_BACKEND
    assert layers["default"].config == {}


def test_load_backend_class_resolves_builtin_backend() -> None:
    backend_class = load_backend_class(DEFAULT_BACKEND)
    assert backend_class is InMemoryChannelLayer


def test_build_channel_layer_instantiates_backend() -> None:
    layer = build_channel_layer(DEFAULT_BACKEND, {"capacity": 3})
    assert isinstance(layer, InMemoryChannelLayer)
    assert layer.capacity == 3


def test_get_channel_layer_reads_django_style_settings() -> None:
    layer = get_channel_layer(
        {
            "default": {
                "BACKEND": DEFAULT_BACKEND,
                "CONFIG": {"capacity": 5},
            }
        }
    )
    assert isinstance(layer, InMemoryChannelLayer)
    assert layer.capacity == 5


def test_parse_channel_layers_rejects_invalid_config() -> None:
    try:
        parse_channel_layers({"default": {"CONFIG": []}})
    except InvalidChannelLayerConfig as exc:
        assert "CONFIG must be a mapping" in str(exc)
    else:
        raise AssertionError("Expected InvalidChannelLayerConfig")


def test_get_channel_layer_rejects_unknown_alias() -> None:
    try:
        get_channel_layer({}, alias="secondary")
    except InvalidChannelLayerConfig as exc:
        assert "alias 'secondary'" in str(exc)
    else:
        raise AssertionError("Expected InvalidChannelLayerConfig")


def test_parse_channel_layers_from_env_uses_inmemory_defaults() -> None:
    layers = parse_channel_layers_from_env({})
    assert layers["default"].backend == BACKEND_ALIASES["inmemory"]
    assert layers["default"].config == {"capacity": 100}


def test_parse_channel_layers_from_env_supports_redis() -> None:
    layers = parse_channel_layers_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": "redis",
            "FASTAPI_WEBSOCKETS_REDIS_URL": "redis://redis.example:6379/1",
            "FASTAPI_WEBSOCKETS_REDIS_PREFIX": "demo",
            "FASTAPI_WEBSOCKETS_REDIS_CLUSTER": "true",
            "FASTAPI_WEBSOCKETS_REDIS_CHANNEL_EXPIRY": "30",
            "FASTAPI_WEBSOCKETS_REDIS_GROUP_EXPIRY": "900",
            "FASTAPI_WEBSOCKETS_REDIS_USE_PUBSUB": "false",
            "FASTAPI_WEBSOCKETS_REDIS_SHARDED_PUBSUB": "false",
        }
    )
    settings = layers["default"]
    assert settings.backend == BACKEND_ALIASES["redis"]
    assert settings.config == {
        "url": "redis://redis.example:6379/1",
        "prefix": "demo",
        "cluster": True,
        "channel_expiry": 30,
        "group_expiry": 900,
        "use_pubsub": False,
        "sharded_pubsub": False,
    }


def test_parse_channel_layers_from_env_supports_postgresql() -> None:
    layers = parse_channel_layers_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": "postgresql",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_DSN": "postgresql://user:pass@db:5432/app",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_SCHEMA": "ws",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_CHANNEL_EXPIRY": "10",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_GROUP_EXPIRY": "20",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_POLL_INTERVAL": "0.25",
            "FASTAPI_WEBSOCKETS_POSTGRESQL_ENSURE_SCHEMA": "no",
        }
    )
    assert layers["default"].backend == BACKEND_ALIASES["postgresql"]
    assert layers["default"].config == {
        "dsn": "postgresql://user:pass@db:5432/app",
        "schema": "ws",
        "channel_expiry": 10,
        "group_expiry": 20,
        "poll_interval": 0.25,
        "ensure_schema": False,
    }


def test_parse_channel_layers_from_env_supports_nats() -> None:
    layers = parse_channel_layers_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": "nats",
            "FASTAPI_WEBSOCKETS_NATS_SERVERS": "nats://one:4222, nats://two:4222",
            "FASTAPI_WEBSOCKETS_NATS_PREFIX": "demo",
            "FASTAPI_WEBSOCKETS_NATS_GROUP_BUCKET": "groups",
            "FASTAPI_WEBSOCKETS_NATS_STREAM_NAME": "STREAM",
            "FASTAPI_WEBSOCKETS_NATS_MESSAGE_TIMEOUT": "15.5",
        }
    )
    assert layers["default"].backend == BACKEND_ALIASES["nats"]
    assert layers["default"].config == {
        "servers": ["nats://one:4222", "nats://two:4222"],
        "prefix": "demo",
        "group_bucket": "groups",
        "stream_name": "STREAM",
        "message_timeout": 15.5,
    }


def test_parse_channel_layers_from_env_supports_rabbitmq() -> None:
    layers = parse_channel_layers_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": "rabbitmq",
            "FASTAPI_WEBSOCKETS_RABBITMQ_URL": "amqp://user:pass@mq:5672//",
            "FASTAPI_WEBSOCKETS_RABBITMQ_EXCHANGE_NAME": "ws",
            "FASTAPI_WEBSOCKETS_RABBITMQ_QUEUE_PREFIX": "queue",
            "FASTAPI_WEBSOCKETS_RABBITMQ_DURABLE": "0",
            "FASTAPI_WEBSOCKETS_RABBITMQ_MESSAGE_TTL": "",
        }
    )
    assert layers["default"].backend == BACKEND_ALIASES["rabbitmq"]
    assert layers["default"].config == {
        "url": "amqp://user:pass@mq:5672//",
        "exchange_name": "ws",
        "queue_prefix": "queue",
        "durable": False,
        "message_ttl": None,
    }


def test_parse_channel_layers_from_env_accepts_full_backend_path() -> None:
    layers = parse_channel_layers_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": DEFAULT_BACKEND,
            "FASTAPI_WEBSOCKETS_INMEMORY_CAPACITY": "7",
        }
    )
    assert layers["default"].backend == DEFAULT_BACKEND
    assert layers["default"].config == {"capacity": 7}


def test_get_channel_layer_from_env_builds_backend() -> None:
    layer = get_channel_layer_from_env(
        {
            "FASTAPI_WEBSOCKETS_BACKEND": "inmemory",
            "FASTAPI_WEBSOCKETS_INMEMORY_CAPACITY": "9",
        }
    )
    assert isinstance(layer, InMemoryChannelLayer)
    assert layer.capacity == 9


def test_parse_channel_layers_from_env_rejects_invalid_bool() -> None:
    try:
        parse_channel_layers_from_env(
            {
                "FASTAPI_WEBSOCKETS_BACKEND": "redis",
                "FASTAPI_WEBSOCKETS_REDIS_CLUSTER": "maybe",
            }
        )
    except InvalidChannelLayerConfig as exc:
        assert "REDIS_CLUSTER" in str(exc)
    else:
        raise AssertionError("Expected InvalidChannelLayerConfig")
