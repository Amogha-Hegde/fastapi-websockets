class ChannelLayerError(Exception):
    """Base error for channel layer operations."""


class InvalidChannelLayerConfig(ChannelLayerError):
    """Raised when a channel layer configuration is invalid."""


class ChannelFull(ChannelLayerError):
    """Raised when a backend cannot enqueue more messages."""


class ChannelLayerClosed(ChannelLayerError):
    """Raised when an operation is attempted on a closed layer."""
