"""
Bolt Database Python Client

A Python client library for connecting to Bolt key-value database clusters.

Example usage:
    from bolt_client import BoltClient

    # Single node connection
    client = BoltClient(host="127.0.0.1", port=8518, username="admin", password="admin")
    client.put("key", "value")
    value = client.get("key")

    # Cluster connection (multi-master)
    client = BoltClient.cluster(
        nodes=["127.0.0.1:8518", "127.0.0.1:8528", "127.0.0.1:8538"],
        username="admin",
        password="admin"
    )
    client.put("key", "value")  # All nodes accept writes
"""

from .client import BoltClient
from .exceptions import (
    BoltError,
    ConnectionError,
    AuthenticationError,
    KeyNotFoundError,
    PermissionError,
    ClusterError,
    UnsupportedOperationError,
)

__version__ = "0.1.0"
__all__ = [
    "BoltClient",
    "BoltError",
    "ConnectionError",
    "AuthenticationError",
    "KeyNotFoundError",
    "PermissionError",
    "ClusterError",
    "UnsupportedOperationError",
]
