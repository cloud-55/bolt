"""
Bolt Agents - High-level agent coordination for Bolt database.

This package provides TaskCoordinator for building multi-agent systems
that use Bolt for task distribution and state management.

Example usage:
    from bolt_client import BoltClient
    from bolt_agents import TaskCoordinator, Task

    client = BoltClient(host="localhost", port=2012, username="admin", password="secret")
    client.connect()

    coord = TaskCoordinator(client)

    # Subscribe to tasks (push notification, no polling)
    def handle_research(task: Task):
        result = do_research(task.data["query"])
        coord.complete_task(task.id, result)

    coord.subscribe("research", agent_id="research_agent_01", callback=handle_research)

    # Or create tasks for other agents
    task_id = coord.create_task("summarize", {"text": "..."})
"""

from .models import Task, TaskStatus
from .coordinator import TaskCoordinator
from .fallback import PollingFallback

__version__ = "0.1.0"
__all__ = [
    "Task",
    "TaskStatus",
    "TaskCoordinator",
    "PollingFallback",
]
