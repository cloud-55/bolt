"""Data models for agent coordination."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class TaskStatus(str, Enum):
    """Task status in the coordination system."""
    PENDING = "Pending"
    CLAIMED = "Claimed"
    COMPLETED = "Completed"
    FAILED = "Failed"


@dataclass
class Task:
    """
    A task in the agent coordination system.

    Tasks are created by orchestrators or other agents and distributed
    to subscribed agents for execution.

    Attributes:
        id: Unique task identifier
        task_type: Type of task (e.g., "research", "summarize")
        data: Task payload data
        status: Current status
        agent_id: ID of the agent that claimed this task
        result: Result data (when completed)
        error: Error message (when failed)
        created_at: Unix timestamp of creation
        updated_at: Unix timestamp of last update
    """
    id: str
    task_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    agent_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: int = 0
    updated_at: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create Task from dictionary (e.g., from JSON response)."""
        # Parse nested data if it's a string
        task_data = data.get("data", {})
        if isinstance(task_data, str):
            import json
            try:
                task_data = json.loads(task_data)
            except json.JSONDecodeError:
                task_data = {"raw": task_data}

        # Parse result if it's a string
        result = data.get("result")
        if isinstance(result, str):
            import json
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                result = {"raw": result}

        # Parse status
        status_str = data.get("status", "Pending")
        try:
            status = TaskStatus(status_str)
        except ValueError:
            status = TaskStatus.PENDING

        return cls(
            id=data.get("id", ""),
            task_type=data.get("task_type", ""),
            data=task_data,
            status=status,
            agent_id=data.get("agent_id"),
            result=result,
            error=data.get("error"),
            created_at=data.get("created_at", 0),
            updated_at=data.get("updated_at", 0),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert Task to dictionary."""
        return {
            "id": self.id,
            "task_type": self.task_type,
            "data": self.data,
            "status": self.status.value,
            "agent_id": self.agent_id,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
