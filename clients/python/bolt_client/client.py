"""Bolt database client."""

import json
import socket
import struct
from typing import Any, Dict, List, Optional, Tuple, Union

from .exceptions import (
    AuthenticationError,
    ClusterError,
    ConnectionError,
    KeyNotFoundError,
    PermissionError,
    ProtocolError,
)
from .protocol import HEADER_SIZE, Message, OpCode


class BoltConnection:
    """Low-level connection to a Bolt server."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 2012,
        timeout: float = 30.0,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None

    def connect(self) -> None:
        """Establish connection to server."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.timeout)
            self._socket.connect((self.host, self.port))
        except socket.error as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    def close(self) -> None:
        """Close connection."""
        if self._socket:
            try:
                self._socket.close()
            except socket.error:
                pass
            self._socket = None

    def is_connected(self) -> bool:
        """Check if connected."""
        return self._socket is not None

    def send(self, message: Message) -> None:
        """Send a message to server."""
        if not self._socket:
            raise ConnectionError("Not connected")

        data = message.encode()
        try:
            self._socket.sendall(data)
        except socket.error as e:
            self.close()
            raise ConnectionError(f"Failed to send message: {e}")

    def receive(self) -> Message:
        """Receive a message from server."""
        if not self._socket:
            raise ConnectionError("Not connected")

        try:
            # Read header (14 bytes): code(2) + db_id_len(4) + key_len(4) + value_len(4)
            header = self._recv_exact(HEADER_SIZE)
            code, db_id_len, key_len, value_len = struct.unpack('>HIII', header)

            # Calculate body size and read body
            body_size = db_id_len + key_len + value_len
            body = self._recv_exact(body_size) if body_size > 0 else b''

            # Parse body
            offset = 0

            database_id = ""
            if db_id_len > 0:
                database_id = body[offset:offset + db_id_len].decode('utf-8')
                offset += db_id_len

            key = body[offset:offset + key_len].decode('utf-8') if key_len > 0 else ""
            offset += key_len

            value = body[offset:offset + value_len].decode('utf-8') if value_len > 0 else ""

            # Determine not_found based on code 0 (not_found_response in Rust)
            not_found = (code == 0)

            return Message(
                code=code,
                key=key,
                value=value,
                not_found=not_found,
                database_id=database_id,
            )

        except socket.error as e:
            self.close()
            raise ConnectionError(f"Failed to receive message: {e}")

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        data = b''
        while len(data) < n:
            chunk = self._socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by server")
            data += chunk
        return data

    def send_receive(self, message: Message) -> Message:
        """Send a message and receive response."""
        self.send(message)
        return self.receive()


class BoltClient:
    """
    Bolt database client with cluster support.

    Example usage:
        # Single node
        client = BoltClient(host="127.0.0.1", port=2012, username="admin", password="admin")

        # Cluster mode
        client = BoltClient.cluster(
            nodes=["127.0.0.1:2012", "127.0.0.1:2013", "127.0.0.1:2014"],
            username="admin",
            password="admin"
        )

        # Operations
        client.put("key", "value")
        value = client.get("key")
        client.delete("key")
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 2012,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        timeout: float = 30.0,
        auto_reconnect: bool = True,
    ):
        self._nodes: List[Tuple[str, int]] = [(host, port)]
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._connection: Optional[BoltConnection] = None
        self._leader_node: Optional[Tuple[str, int]] = None
        self._cluster_mode = False

    @classmethod
    def cluster(
        cls,
        nodes: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        timeout: float = 30.0,
    ) -> 'BoltClient':
        """
        Create a cluster-aware client.

        Args:
            nodes: List of node addresses in "host:port" format
            username: Authentication username
            password: Authentication password
            database: Default database to use
            timeout: Connection timeout in seconds

        Returns:
            BoltClient configured for cluster mode
        """
        if not nodes:
            raise ValueError("At least one node must be specified")

        parsed_nodes = []
        for node in nodes:
            parts = node.split(':')
            if len(parts) == 2:
                host = parts[0]
                port = int(parts[1])
            else:
                host = node
                port = 2012
            parsed_nodes.append((host, port))

        client = cls(
            host=parsed_nodes[0][0],
            port=parsed_nodes[0][1],
            username=username,
            password=password,
            database=database,
            timeout=timeout,
        )
        client._nodes = parsed_nodes
        client._cluster_mode = True
        return client

    def connect(self) -> None:
        """Connect to the server or cluster."""
        if self._cluster_mode:
            self._discover_leader()
        else:
            self._connect_to_node(self._nodes[0][0], self._nodes[0][1])

    def _connect_to_node(self, host: str, port: int) -> None:
        """Connect to a specific node."""
        if self._connection:
            self._connection.close()

        self._connection = BoltConnection(host, port, self._timeout)
        self._connection.connect()

        # Authenticate if credentials provided
        if self._username and self._password:
            self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the server."""
        if not self._connection:
            raise ConnectionError("Not connected")

        auth_msg = Message.auth(self._username, self._password)
        response = self._connection.send_receive(auth_msg)

        if response.code == OpCode.AUTH_FAIL:
            self._connection.close()
            raise AuthenticationError(response.value or "Authentication failed")

        if response.code != OpCode.AUTH_OK:
            self._connection.close()
            raise ProtocolError(f"Unexpected auth response code: {response.code}")

    def _discover_leader(self) -> None:
        """Discover the cluster leader."""
        last_error = None

        for host, port in self._nodes:
            try:
                self._connect_to_node(host, port)
                status = self._get_cluster_status()

                if status.get('role') == 'leader':
                    # Already connected to leader
                    self._leader_node = (host, port)
                    return

                # Get leader info and connect to it
                leader_id = status.get('leader')
                if leader_id and leader_id != 'unknown':
                    # Find leader in peers
                    for peer in status.get('peers', []):
                        if peer.get('id') == leader_id:
                            leader_host = peer.get('host')
                            leader_port = peer.get('port')
                            if leader_host and leader_port:
                                self._connection.close()
                                self._connect_to_node(leader_host, leader_port)
                                self._leader_node = (leader_host, leader_port)
                                return

                # If we're here, we're connected to a follower but couldn't find leader
                # This node will work for reads, keep it
                self._leader_node = (host, port)
                return

            except (ConnectionError, AuthenticationError) as e:
                last_error = e
                continue

        if last_error:
            raise ClusterError(f"Failed to connect to any cluster node: {last_error}")
        else:
            raise ClusterError("No cluster nodes available")

    def _get_cluster_status(self) -> Dict[str, Any]:
        """Get cluster status from connected node."""
        msg = Message.cluster_status()
        response = self._connection.send_receive(msg)

        if response.code == OpCode.CLUSTER_STATUS:
            try:
                return json.loads(response.value)
            except json.JSONDecodeError:
                return {}
        return {}

    def _ensure_connected(self) -> None:
        """Ensure we have a valid connection."""
        if not self._connection or not self._connection.is_connected():
            self.connect()

    def _execute(self, message: Message) -> Message:
        """Execute a command and return response."""
        self._ensure_connected()

        try:
            return self._connection.send_receive(message)
        except ConnectionError:
            if self._auto_reconnect:
                self.connect()
                return self._connection.send_receive(message)
            raise

    def close(self) -> None:
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> 'BoltClient':
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ==================== Key-Value Operations ====================

    def put(self, key: str, value: str, database: Optional[str] = None) -> None:
        """
        Set a key-value pair.

        Args:
            key: The key to set
            value: The value to set
            database: Optional database name (uses default if not specified)
        """
        db = database or self._database or ""
        msg = Message(
            code=OpCode.PUT,
            key=key,
            value=value,
            database_id=db,
        )
        response = self._execute(msg)
        self._check_permission(response)

    def get(self, key: str, database: Optional[str] = None) -> Optional[str]:
        """
        Get a value by key.

        Args:
            key: The key to get
            database: Optional database name

        Returns:
            The value if found, None if not found
        """
        db = database or self._database
        msg = Message(
            code=OpCode.GET,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)

        if response.not_found:
            return None
        return response.value

    def delete(self, key: str, database: Optional[str] = None) -> Optional[str]:
        """
        Delete a key.

        Args:
            key: The key to delete
            database: Optional database name

        Returns:
            The deleted value if found, None if not found
        """
        db = database or self._database
        msg = Message(
            code=OpCode.DEL,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)

        if response.not_found:
            return None
        return response.value

    def setex(self, key: str, value: str, ttl_seconds: int, database: Optional[str] = None) -> None:
        """
        Set a key with TTL (expiration).

        Args:
            key: The key to set
            value: The value to set
            ttl_seconds: Time to live in seconds
            database: Optional database name
        """
        db = database or self._database
        msg = Message(
            code=OpCode.SETEX,
            key=key,
            value=f"{ttl_seconds}:{value}",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)

    def ttl(self, key: str, database: Optional[str] = None) -> Optional[int]:
        """
        Get TTL of a key.

        Args:
            key: The key
            database: Optional database name

        Returns:
            TTL in seconds, -1 if no expiration, None if key not found
        """
        db = database or self._database
        msg = Message(
            code=OpCode.TTL,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)

        if response.not_found:
            return None
        return int(response.value)

    def exists(self, key: str, database: Optional[str] = None) -> bool:
        """Check if a key exists."""
        db = database or self._database
        msg = Message(
            code=OpCode.EXISTS,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        return response.value == "1"

    def key_type(self, key: str, database: Optional[str] = None) -> Optional[str]:
        """Get the type of a key."""
        db = database or self._database
        msg = Message(
            code=OpCode.TYPE,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        if response.not_found or response.value == "none":
            return None
        return response.value

    def keys(self, pattern: str = "*", database: Optional[str] = None) -> List[str]:
        """Get keys matching pattern."""
        db = database or self._database
        msg = Message(
            code=OpCode.KEYS,
            key=pattern,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        if not response.value:
            return []
        return response.value.split('\n')

    # ==================== Batch Operations ====================

    def mget(self, keys: List[str], database: Optional[str] = None) -> List[Optional[str]]:
        """Get multiple keys at once."""
        db = database or self._database
        msg = Message(
            code=OpCode.MGET,
            key='\n'.join(keys),
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        values = response.value.split('\n')
        return [v if v else None for v in values]

    def mset(self, pairs: Dict[str, str], database: Optional[str] = None) -> int:
        """Set multiple key-value pairs at once."""
        db = database or self._database
        keys = '\n'.join(pairs.keys())
        values = '\n'.join(pairs.values())
        msg = Message(
            code=OpCode.MSET,
            key=keys,
            value=values,
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def mdel(self, keys: List[str], database: Optional[str] = None) -> int:
        """Delete multiple keys at once."""
        db = database or self._database
        msg = Message(
            code=OpCode.MDEL,
            key='\n'.join(keys),
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    # ==================== Counter Operations ====================

    def incr(self, key: str, database: Optional[str] = None) -> int:
        """Increment a counter by 1."""
        db = database or self._database
        msg = Message(
            code=OpCode.INCR,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def decr(self, key: str, database: Optional[str] = None) -> int:
        """Decrement a counter by 1."""
        db = database or self._database
        msg = Message(
            code=OpCode.DECR,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def incrby(self, key: str, delta: int, database: Optional[str] = None) -> int:
        """Increment a counter by a specific amount."""
        db = database or self._database
        msg = Message(
            code=OpCode.INCRBY,
            key=key,
            value=str(delta),
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    # ==================== List Operations ====================

    def lpush(self, key: str, *values: str, database: Optional[str] = None) -> int:
        """Push values to the head of a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.LPUSH,
            key=key,
            value='\n'.join(values),
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def rpush(self, key: str, *values: str, database: Optional[str] = None) -> int:
        """Push values to the tail of a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.RPUSH,
            key=key,
            value='\n'.join(values),
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def lpop(self, key: str, database: Optional[str] = None) -> Optional[str]:
        """Pop value from the head of a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.LPOP,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        if response.not_found:
            return None
        return response.value

    def rpop(self, key: str, database: Optional[str] = None) -> Optional[str]:
        """Pop value from the tail of a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.RPOP,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        if response.not_found:
            return None
        return response.value

    def lrange(self, key: str, start: int = 0, stop: int = -1, database: Optional[str] = None) -> List[str]:
        """Get a range of elements from a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.LRANGE,
            key=key,
            value=f"{start}:{stop}",
            database_id=db or "",
        )
        response = self._execute(msg)
        if not response.value:
            return []
        return response.value.split('\n')

    def llen(self, key: str, database: Optional[str] = None) -> int:
        """Get the length of a list."""
        db = database or self._database
        msg = Message(
            code=OpCode.LLEN,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        return int(response.value)

    # ==================== Set Operations ====================

    def sadd(self, key: str, *members: str, database: Optional[str] = None) -> int:
        """Add members to a set."""
        db = database or self._database
        msg = Message(
            code=OpCode.SADD,
            key=key,
            value='\n'.join(members),
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def srem(self, key: str, *members: str, database: Optional[str] = None) -> int:
        """Remove members from a set."""
        db = database or self._database
        msg = Message(
            code=OpCode.SREM,
            key=key,
            value='\n'.join(members),
            database_id=db or "",
        )
        response = self._execute(msg)
        self._check_permission(response)
        return int(response.value)

    def smembers(self, key: str, database: Optional[str] = None) -> List[str]:
        """Get all members of a set."""
        db = database or self._database
        msg = Message(
            code=OpCode.SMEMBERS,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        if not response.value:
            return []
        return response.value.split('\n')

    def scard(self, key: str, database: Optional[str] = None) -> int:
        """Get the cardinality (size) of a set."""
        db = database or self._database
        msg = Message(
            code=OpCode.SCARD,
            key=key,
            value="",
            database_id=db or "",
        )
        response = self._execute(msg)
        return int(response.value)

    def sismember(self, key: str, member: str, database: Optional[str] = None) -> bool:
        """Check if a member exists in a set."""
        db = database or self._database
        msg = Message(
            code=OpCode.SISMEMBER,
            key=key,
            value=member,
            database_id=db or "",
        )
        response = self._execute(msg)
        return response.value == "1"

    # ==================== Server Operations ====================

    def stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        msg = Message(code=OpCode.STATS, key="", value="")
        response = self._execute(msg)
        try:
            return json.loads(response.value)
        except json.JSONDecodeError:
            return {}

    def metrics(self) -> str:
        """Get Prometheus metrics."""
        msg = Message(code=OpCode.METRICS, key="", value="")
        response = self._execute(msg)
        return response.value

    def cluster_status(self) -> Dict[str, Any]:
        """Get cluster status."""
        msg = Message(code=OpCode.CLUSTER_STATUS, key="", value="")
        response = self._execute(msg)
        try:
            return json.loads(response.value)
        except json.JSONDecodeError:
            return {}

    # ==================== Helpers ====================

    def _check_permission(self, response: Message) -> None:
        """Check if response indicates permission error."""
        if response.code == OpCode.AUTH_FAIL:
            raise PermissionError(response.value)
