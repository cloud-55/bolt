#!/usr/bin/env python3
"""Load test for Bolt - writes 500,000 records."""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import Lock
from typing import Optional

from bolt_client import BoltClient, ConnectionError, AuthenticationError


@dataclass
class Stats:
    """Statistics collector."""
    total: int = 0
    success: int = 0
    errors: int = 0
    start_time: float = 0.0
    lock: Lock = None

    def __post_init__(self):
        self.lock = Lock()

    def record_success(self):
        with self.lock:
            self.success += 1

    def record_error(self):
        with self.lock:
            self.errors += 1

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def ops_per_second(self) -> float:
        elapsed = self.elapsed
        if elapsed == 0:
            return 0
        return self.success / elapsed

    @property
    def progress(self) -> float:
        if self.total == 0:
            return 0
        return (self.success + self.errors) / self.total * 100


def create_client(host: str, port: int, username: str, password: str) -> Optional[BoltClient]:
    """Create and connect a Bolt client."""
    try:
        client = BoltClient(host=host, port=port, username=username, password=password)
        client.connect()
        return client
    except (ConnectionError, AuthenticationError) as e:
        print(f"Failed to connect: {e}")
        return None


def write_batch(
    client: BoltClient,
    start_idx: int,
    count: int,
    prefix: str,
    stats: Stats
) -> int:
    """Write a batch of records."""
    success = 0
    for i in range(start_idx, start_idx + count):
        try:
            key = f"{prefix}:{i}"
            value = f"value_{i}_{'x' * 100}"  # ~110 bytes per value
            client.put(key, value)
            stats.record_success()
            success += 1
        except Exception as e:
            stats.record_error()
    return success


def worker(
    worker_id: int,
    host: str,
    port: int,
    username: str,
    password: str,
    start_idx: int,
    count: int,
    prefix: str,
    stats: Stats,
    batch_size: int = 1000
) -> int:
    """Worker function that writes records."""
    client = create_client(host, port, username, password)
    if not client:
        return 0

    try:
        total_success = 0
        for batch_start in range(start_idx, start_idx + count, batch_size):
            batch_count = min(batch_size, start_idx + count - batch_start)
            success = write_batch(client, batch_start, batch_count, prefix, stats)
            total_success += success
        return total_success
    finally:
        client.close()


def print_progress(stats: Stats, final: bool = False):
    """Print progress bar and stats."""
    bar_width = 40
    filled = int(bar_width * stats.progress / 100)
    bar = '=' * filled + '-' * (bar_width - filled)

    line = f"\r[{bar}] {stats.progress:5.1f}% | "
    line += f"{stats.success:,}/{stats.total:,} | "
    line += f"{stats.ops_per_second:,.0f} ops/s | "
    line += f"Errors: {stats.errors:,}"

    if final:
        print(line)
    else:
        print(line, end='', flush=True)


def run_load_test(
    host: str = "127.0.0.1",
    port: int = 8518,
    username: str = "admin",
    password: str = "admin",
    total_records: int = 500_000,
    num_workers: int = 10,
    prefix: str = "loadtest"
):
    """Run the load test."""
    print(f"\n{'='*60}")
    print(f"Bolt Load Test")
    print(f"{'='*60}")
    print(f"Target:     {host}:{port}")
    print(f"Records:    {total_records:,}")
    print(f"Workers:    {num_workers}")
    print(f"Key prefix: {prefix}")
    print(f"{'='*60}\n")

    # Test connection first
    print("Testing connection...")
    test_client = create_client(host, port, username, password)
    if not test_client:
        print("Failed to connect to Bolt server. Exiting.")
        sys.exit(1)
    test_client.close()
    print("Connection OK\n")

    # Initialize stats
    stats = Stats(total=total_records)
    stats.start_time = time.time()

    # Calculate records per worker
    records_per_worker = total_records // num_workers
    remainder = total_records % num_workers

    print("Starting load test...\n")

    # Start workers
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        start_idx = 0

        for worker_id in range(num_workers):
            # Distribute remainder among first workers
            count = records_per_worker + (1 if worker_id < remainder else 0)

            future = executor.submit(
                worker,
                worker_id,
                host,
                port,
                username,
                password,
                start_idx,
                count,
                prefix,
                stats
            )
            futures.append(future)
            start_idx += count

        # Monitor progress
        while not all(f.done() for f in futures):
            print_progress(stats)
            time.sleep(0.5)

        # Wait for all to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"\nWorker error: {e}")

    # Final stats
    print_progress(stats, final=True)

    elapsed = stats.elapsed
    print(f"\n{'='*60}")
    print(f"Results")
    print(f"{'='*60}")
    print(f"Total time:     {elapsed:.2f} seconds")
    print(f"Records:        {stats.success:,} successful, {stats.errors:,} errors")
    print(f"Throughput:     {stats.ops_per_second:,.0f} ops/second")
    print(f"Avg latency:    {(elapsed / stats.success * 1000):.2f} ms/op" if stats.success > 0 else "N/A")
    print(f"{'='*60}\n")

    return stats


def cleanup(
    host: str = "127.0.0.1",
    port: int = 8518,
    username: str = "admin",
    password: str = "admin",
    prefix: str = "loadtest"
):
    """Clean up test keys."""
    print(f"Cleaning up keys with prefix '{prefix}'...")
    client = create_client(host, port, username, password)
    if not client:
        return

    try:
        keys = client.keys(f"{prefix}:*")
        if keys:
            print(f"Found {len(keys):,} keys to delete...")
            # Delete in batches
            batch_size = 1000
            for i in range(0, len(keys), batch_size):
                batch = keys[i:i + batch_size]
                client.mdel(batch)
                print(f"\rDeleted {min(i + batch_size, len(keys)):,}/{len(keys):,}", end='', flush=True)
            print("\nCleanup complete.")
        else:
            print("No keys to clean up.")
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Bolt Load Test")
    parser.add_argument("--host", default="127.0.0.1", help="Bolt server host")
    parser.add_argument("--port", type=int, default=8518, help="Bolt server port")
    parser.add_argument("--username", default="admin", help="Username")
    parser.add_argument("--password", default="admin", help="Password")
    parser.add_argument("--records", type=int, default=500_000, help="Number of records to write")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--prefix", default="loadtest", help="Key prefix")
    parser.add_argument("--cleanup", action="store_true", help="Clean up test keys after")
    parser.add_argument("--cleanup-only", action="store_true", help="Only clean up, don't run test")

    args = parser.parse_args()

    if args.cleanup_only:
        cleanup(args.host, args.port, args.username, args.password, args.prefix)
        return

    stats = run_load_test(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        total_records=args.records,
        num_workers=args.workers,
        prefix=args.prefix
    )

    if args.cleanup:
        cleanup(args.host, args.port, args.username, args.password, args.prefix)

    # Exit with error code if there were failures
    if stats.errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
