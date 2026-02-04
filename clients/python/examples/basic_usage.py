#!/usr/bin/env python3
"""Basic usage example for Bolt Python client."""

from bolt_client import BoltClient, ConnectionError, AuthenticationError


def main():
    # Single node connection
    print("=== Single Node Connection ===")
    client = BoltClient(
        host="127.0.0.1",
        port=8518,
        username="admin",
        password="admin"
    )

    try:
        client.connect()
        print("Connected to Bolt server")

        # Basic key-value operations
        print("\n--- Key-Value Operations ---")
        client.put("name", "Alice")
        print(f"PUT name = Alice")

        name = client.get("name")
        print(f"GET name = {name}")

        exists = client.exists("name")
        print(f"EXISTS name = {exists}")

        # Counter operations
        print("\n--- Counter Operations ---")
        client.put("counter", "0")
        val = client.incr("counter")
        print(f"INCR counter = {val}")
        val = client.incr("counter")
        print(f"INCR counter = {val}")
        val = client.incrby("counter", 10)
        print(f"INCRBY counter 10 = {val}")

        # List operations
        print("\n--- List Operations ---")
        client.rpush("mylist", "a", "b", "c")
        print("RPUSH mylist a b c")
        items = client.lrange("mylist", 0, -1)
        print(f"LRANGE mylist 0 -1 = {items}")
        item = client.lpop("mylist")
        print(f"LPOP mylist = {item}")

        # Set operations
        print("\n--- Set Operations ---")
        client.sadd("myset", "x", "y", "z")
        print("SADD myset x y z")
        members = client.smembers("myset")
        print(f"SMEMBERS myset = {members}")
        is_member = client.sismember("myset", "y")
        print(f"SISMEMBER myset y = {is_member}")

        # TTL operations
        print("\n--- TTL Operations ---")
        client.setex("session", "data", 60)
        print("SETEX session data 60")
        ttl = client.ttl("session")
        print(f"TTL session = {ttl}s")

        # Batch operations
        print("\n--- Batch Operations ---")
        client.mset({"key1": "val1", "key2": "val2", "key3": "val3"})
        print("MSET key1=val1 key2=val2 key3=val3")
        values = client.mget(["key1", "key2", "key3"])
        print(f"MGET key1 key2 key3 = {values}")

        # Server info
        print("\n--- Server Info ---")
        stats = client.stats()
        print(f"Stats: {stats}")

        cluster = client.cluster_status()
        print(f"Cluster: {cluster}")

        # Cleanup
        print("\n--- Cleanup ---")
        client.delete("name")
        client.delete("counter")
        client.delete("mylist")
        client.delete("myset")
        client.delete("session")
        client.mdel(["key1", "key2", "key3"])
        print("Cleaned up test keys")

    except ConnectionError as e:
        print(f"Connection error: {e}")
    except AuthenticationError as e:
        print(f"Authentication error: {e}")
    finally:
        client.close()
        print("\nConnection closed")


def cluster_example():
    """Example of cluster usage."""
    print("\n=== Cluster Connection ===")

    # Connect to cluster
    client = BoltClient.cluster(
        nodes=["127.0.0.1:8518", "127.0.0.1:8528", "127.0.0.1:8538"],
        username="admin",
        password="admin"
    )

    try:
        client.connect()
        print("Connected to Bolt cluster")

        # Check cluster status
        status = client.cluster_status()
        print(f"Node role: {status.get('role')}")
        print(f"Peers: {status.get('peers')}")

        # Write to any node (multi-master)
        client.put("cluster_test", "hello from cluster")
        print("PUT cluster_test = hello from cluster")

        # Read back
        value = client.get("cluster_test")
        print(f"GET cluster_test = {value}")

        # Cleanup
        client.delete("cluster_test")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()

    # Uncomment to test cluster mode
    # cluster_example()
