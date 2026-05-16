from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping


class BaseChannelLayer(ABC):
    """Common async interface shared by all channel layer backends."""

    def __init__(self, **config: Any) -> None:
        self.config = dict(config)

    @abstractmethod
    async def send(self, channel: str, message: Mapping[str, Any]) -> None:
        """Send a message to a single channel."""

    @abstractmethod
    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Mapping[str, Any]:
        """Receive a message from a single channel."""

    @abstractmethod
    async def new_channel(self, prefix: str = "specific") -> str:
        """Create and return a new channel name."""

    @abstractmethod
    async def group_add(self, group: str, channel: str) -> None:
        """Attach a channel to a named group."""

    @abstractmethod
    async def group_discard(self, group: str, channel: str) -> None:
        """Detach a channel from a named group."""

    @abstractmethod
    async def group_send(self, group: str, message: Mapping[str, Any]) -> None:
        """Fan out a message to all channels in a group."""

    @abstractmethod
    async def close(self) -> None:
        """Release backend resources."""
