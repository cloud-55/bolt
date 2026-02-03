"""Polling fallback for servers without Pub/Sub support."""

import logging
import threading
import time
from typing import Callable, Optional

from bolt_client import BoltClient

from .models import Task

logger = logging.getLogger(__name__)


class PollingFallback:
    """
    Polling-based task retrieval for graceful degradation.

    When Pub/Sub is not available, this class provides polling with
    exponential backoff to reduce server load during idle periods.

    The polling interval starts at 1 second and doubles on each empty
    poll, up to a maximum of 30 seconds. When a task is found, the
    interval resets to 1 second.
    """

    DEFAULT_INITIAL_INTERVAL = 1.0  # seconds
    DEFAULT_MAX_INTERVAL = 30.0  # seconds
    DEFAULT_BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        client: BoltClient,
        task_type: str,
        agent_id: str,
        callback: Callable[[Task], None],
        initial_interval: float = DEFAULT_INITIAL_INTERVAL,
        max_interval: float = DEFAULT_MAX_INTERVAL,
        backoff_multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
    ):
        """
        Initialize polling fallback.

        Args:
            client: Connected BoltClient
            task_type: Type of tasks to poll for
            agent_id: Agent ID for claiming tasks
            callback: Function called when a task is claimed
            initial_interval: Starting poll interval in seconds
            max_interval: Maximum poll interval in seconds
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.client = client
        self.task_type = task_type
        self.agent_id = agent_id
        self.callback = callback
        self.initial_interval = initial_interval
        self.max_interval = max_interval
        self.backoff_multiplier = backoff_multiplier

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_interval = initial_interval

    def start(self) -> None:
        """Start the polling loop in a background thread."""
        if self._running:
            return

        self._running = True
        self._current_interval = self.initial_interval
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"PollingFallback-{self.task_type}",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.max_interval + 1)
            self._thread = None

    def _poll_loop(self) -> None:
        """Main polling loop with exponential backoff."""
        logger.debug(f"Starting poll loop for '{self.task_type}'")

        while self._running:
            try:
                task = self._try_claim_task()

                if task:
                    # Task found! Reset interval and call callback
                    self._current_interval = self.initial_interval
                    logger.debug(f"Claimed task {task.id}, calling callback")

                    try:
                        self.callback(task)
                    except Exception as e:
                        logger.error(f"Callback error for task {task.id}: {e}")
                else:
                    # No task, increase interval (exponential backoff)
                    self._current_interval = min(
                        self._current_interval * self.backoff_multiplier,
                        self.max_interval,
                    )
                    logger.debug(
                        f"No tasks for '{self.task_type}', "
                        f"next poll in {self._current_interval:.1f}s"
                    )

            except Exception as e:
                logger.error(f"Poll error for '{self.task_type}': {e}")
                # On error, use max interval to avoid hammering the server
                self._current_interval = self.max_interval

            # Sleep with early exit check
            self._interruptible_sleep(self._current_interval)

        logger.debug(f"Poll loop stopped for '{self.task_type}'")

    def _try_claim_task(self) -> Optional[Task]:
        """
        Try to find and claim a pending task.

        Returns:
            Claimed Task if successful, None otherwise
        """
        # List pending tasks of our type
        try:
            tasks = self.client.list_tasks(self.task_type, "pending")
        except Exception:
            return None

        if not tasks:
            return None

        # Try to claim the first available task
        for task_data in tasks:
            task_id = task_data.get("id")
            if not task_id:
                continue

            try:
                claimed = self.client.claim_task(task_id, self.agent_id)
                if claimed:
                    return Task.from_dict(claimed)
            except Exception:
                # Task might have been claimed by another agent
                continue

        return None

    def _interruptible_sleep(self, duration: float) -> None:
        """Sleep for duration, but wake up if stop() is called."""
        end_time = time.monotonic() + duration
        while self._running and time.monotonic() < end_time:
            time.sleep(min(0.5, duration))  # Check every 0.5s at most

    @property
    def is_running(self) -> bool:
        """Check if polling is active."""
        return self._running

    @property
    def current_interval(self) -> float:
        """Get current polling interval."""
        return self._current_interval
