"""Task coordinator for agent coordination."""

import json
import logging
from typing import Any, Callable, Dict, List, Optional, Union

from bolt_client import BoltClient, UnsupportedOperationError

from .models import Task, TaskStatus
from .fallback import PollingFallback

logger = logging.getLogger(__name__)


class TaskCoordinator:
    """
    High-level coordinator for multi-agent task distribution.

    TaskCoordinator wraps BoltClient to provide a clean API for:
    - Subscribing to task types (with automatic polling fallback)
    - Creating and distributing tasks
    - Completing or failing tasks

    Example:
        client = BoltClient(...)
        client.connect()

        coord = TaskCoordinator(client)

        # As a worker agent:
        def handle_task(task: Task):
            result = process(task.data)
            coord.complete_task(task.id, result)

        coord.subscribe("my_task_type", "agent_01", handle_task)

        # As an orchestrator:
        task_id = coord.create_task("my_task_type", {"key": "value"})
    """

    def __init__(self, client: BoltClient):
        """
        Initialize TaskCoordinator.

        Args:
            client: Connected BoltClient instance
        """
        self.client = client
        self._use_pubsub = True
        self._fallback_pollers: Dict[str, PollingFallback] = {}
        self._callbacks: Dict[str, Callable[[Task], None]] = {}

    def subscribe(
        self,
        task_type: str,
        agent_id: str,
        callback: Callable[[Task], None],
    ) -> None:
        """
        Subscribe to a task type.

        Agents will receive tasks via push notification when available.
        If the server doesn't support Pub/Sub, falls back to polling.

        Args:
            task_type: Type of tasks to subscribe to
            agent_id: Unique identifier for this agent
            callback: Function called when a task is received
        """
        self._callbacks[task_type] = callback

        if self._use_pubsub:
            try:
                # Wrap callback to convert dict to Task
                def wrapped_callback(data: Dict[str, Any]) -> None:
                    task = Task.from_dict(data)
                    callback(task)

                self.client.subscribe(task_type, agent_id, wrapped_callback)
                logger.info(f"Subscribed to '{task_type}' via Pub/Sub")
                return
            except UnsupportedOperationError:
                logger.warning(
                    f"Pub/Sub not supported, falling back to polling for '{task_type}'"
                )
                self._use_pubsub = False

        # Fallback to polling
        self._start_polling_fallback(task_type, agent_id, callback)

    def unsubscribe(self, task_type: str) -> None:
        """
        Unsubscribe from a task type.

        Args:
            task_type: Type of tasks to unsubscribe from
        """
        # Stop polling if active
        if task_type in self._fallback_pollers:
            self._fallback_pollers[task_type].stop()
            del self._fallback_pollers[task_type]

        # Unsubscribe from Pub/Sub
        if self._use_pubsub:
            try:
                self.client.unsubscribe(task_type)
            except Exception:
                pass

        # Remove callback
        self._callbacks.pop(task_type, None)

    def create_task(
        self,
        task_type: str,
        data: Union[str, Dict[str, Any]],
    ) -> str:
        """
        Create a task for distribution to agents.

        Args:
            task_type: Type of task
            data: Task payload data

        Returns:
            Task ID
        """
        task_id, _ = self.client.create_task(task_type, data)
        return task_id

    def complete_task(
        self,
        task_id: str,
        result: Union[str, Dict[str, Any]],
    ) -> bool:
        """
        Mark a task as completed.

        Args:
            task_id: ID of the task
            result: Result data

        Returns:
            True if successful
        """
        return self.client.complete_task(task_id, result)

    def fail_task(self, task_id: str, error: str) -> bool:
        """
        Mark a task as failed.

        Args:
            task_id: ID of the task
            error: Error message

        Returns:
            True if successful
        """
        return self.client.fail_task(task_id, error)

    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Get task by ID.

        Args:
            task_id: ID of the task

        Returns:
            Task if found, None otherwise
        """
        data = self.client.task_status(task_id)
        if data:
            return Task.from_dict(data)
        return None

    def list_tasks(
        self,
        task_type: Optional[str] = None,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """
        List tasks with optional filters.

        Args:
            task_type: Filter by task type
            status: Filter by status

        Returns:
            List of tasks
        """
        status_str = status.value.lower() if status else None
        data = self.client.list_tasks(task_type, status_str)
        return [Task.from_dict(t) for t in data]

    def claim_task(self, task_id: str, agent_id: str) -> Optional[Task]:
        """
        Manually claim a pending task.

        Args:
            task_id: ID of the task
            agent_id: ID of the claiming agent

        Returns:
            Task if claimed, None if not found or already claimed
        """
        data = self.client.claim_task(task_id, agent_id)
        if data:
            return Task.from_dict(data)
        return None

    def _start_polling_fallback(
        self,
        task_type: str,
        agent_id: str,
        callback: Callable[[Task], None],
    ) -> None:
        """Start polling fallback for a task type."""
        if task_type in self._fallback_pollers:
            self._fallback_pollers[task_type].stop()

        poller = PollingFallback(
            client=self.client,
            task_type=task_type,
            agent_id=agent_id,
            callback=callback,
        )
        self._fallback_pollers[task_type] = poller
        poller.start()
        logger.info(f"Started polling fallback for '{task_type}'")

    def stop(self) -> None:
        """Stop all subscriptions and polling."""
        # Stop all pollers
        for poller in self._fallback_pollers.values():
            poller.stop()
        self._fallback_pollers.clear()

        # Stop Pub/Sub listener
        if self._use_pubsub:
            self.client.stop_listener()

        self._callbacks.clear()

    def __enter__(self) -> "TaskCoordinator":
        return self

    def __exit__(self, *args) -> None:
        self.stop()
