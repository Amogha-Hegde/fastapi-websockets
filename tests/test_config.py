from fastapi_websockets.backends.inmemory import InMemoryChannelLayer
from fastapi_websockets.config import (
    DEFAULT_BACKEND,
    build_channel_layer,
    get_channel_layer,
    load_backend_class,
    parse_channel_layers,
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
