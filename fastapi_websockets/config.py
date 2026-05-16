from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Mapping, Type

from fastapi_websockets.backends.base import BaseChannelLayer
from fastapi_websockets.exceptions import InvalidChannelLayerConfig

DEFAULT_BACKEND = "fastapi_websockets.backends.inmemory.InMemoryChannelLayer"


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


def get_channel_layer(
    settings: Mapping[str, Mapping[str, Any]] | None = None,
    alias: str = "default",
) -> BaseChannelLayer:
    layers = parse_channel_layers(settings)
    if alias not in layers:
        raise InvalidChannelLayerConfig(f"Channel layer alias '{alias}' is not defined")
    layer_settings = layers[alias]
    return build_channel_layer(layer_settings.backend, layer_settings.config)
