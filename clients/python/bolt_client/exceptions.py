"""Bolt client exceptions."""


class BoltError(Exception):
    """Base exception for Bolt client errors."""
    pass


class ConnectionError(BoltError):
    """Failed to connect to Bolt server."""
    pass


class AuthenticationError(BoltError):
    """Authentication failed."""
    pass


class KeyNotFoundError(BoltError):
    """Key not found in database."""
    pass


class PermissionError(BoltError):
    """Permission denied for operation."""
    pass


class ClusterError(BoltError):
    """Cluster-related error."""
    pass


class ProtocolError(BoltError):
    """Protocol error in communication."""
    pass
