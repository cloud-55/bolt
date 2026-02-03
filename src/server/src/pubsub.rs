//! Pub/Sub module for agent coordination
//!
//! Provides subscription-based task distribution to eliminate polling.
//! Agents subscribe to task types and receive notifications via push.

use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};
use serde::{Deserialize, Serialize};

/// Task status in the coordination system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Claimed,
    Completed,
    Failed,
}

/// A task to be distributed to agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub task_type: String,
    pub data: String,
    pub status: TaskStatus,
    pub agent_id: Option<String>,
    pub result: Option<String>,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Task {
    pub fn new(id: String, task_type: String, data: String, timestamp: u64) -> Self {
        Task {
            id,
            task_type,
            data,
            status: TaskStatus::Pending,
            agent_id: None,
            result: None,
            error: None,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }
}

/// Notification sent to subscribed agents
#[derive(Debug, Clone)]
pub struct Notification {
    pub task: Task,
}

/// A subscriber (agent) registered for a task type
struct Subscriber {
    agent_id: String,
    tx: mpsc::Sender<Notification>,
}

/// Manages subscriptions and distributes tasks via round-robin
pub struct SubscriptionManager {
    /// task_type -> list of subscribers
    subscriptions: RwLock<HashMap<String, Vec<Subscriber>>>,
    /// Round-robin index per task type
    rr_index: RwLock<HashMap<String, usize>>,
    /// All tasks by ID
    tasks: RwLock<HashMap<String, Task>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        SubscriptionManager {
            subscriptions: RwLock::new(HashMap::new()),
            rr_index: RwLock::new(HashMap::new()),
            tasks: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe an agent to a task type
    pub async fn subscribe(
        &self,
        task_type: &str,
        agent_id: &str,
        tx: mpsc::Sender<Notification>,
    ) {
        let mut subs = self.subscriptions.write().await;
        let subscribers = subs.entry(task_type.to_string()).or_insert_with(Vec::new);

        // Remove existing subscription for this agent (re-subscribe)
        subscribers.retain(|s| s.agent_id != agent_id);

        subscribers.push(Subscriber {
            agent_id: agent_id.to_string(),
            tx,
        });
    }

    /// Unsubscribe an agent from a task type
    pub async fn unsubscribe(&self, task_type: &str, agent_id: &str) {
        let mut subs = self.subscriptions.write().await;
        if let Some(subscribers) = subs.get_mut(task_type) {
            subscribers.retain(|s| s.agent_id != agent_id);
            if subscribers.is_empty() {
                subs.remove(task_type);
            }
        }
    }

    /// Remove an agent from all subscriptions (on disconnect)
    pub async fn remove_agent(&self, agent_id: &str) {
        let mut subs = self.subscriptions.write().await;
        for subscribers in subs.values_mut() {
            subscribers.retain(|s| s.agent_id != agent_id);
        }
        // Clean up empty task types
        subs.retain(|_, v| !v.is_empty());
    }

    /// Create a task and notify subscribers via round-robin
    /// Returns the task ID and the agent_id that received it (if any)
    pub async fn create_task(&self, task: Task) -> (String, Option<String>) {
        let task_id = task.id.clone();
        let task_type = task.task_type.clone();

        // Store the task
        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(task_id.clone(), task.clone());
        }

        // Try to notify a subscriber
        let agent_id = self.notify_round_robin(&task_type, task).await;

        // Update task status if claimed
        if let Some(ref agent) = agent_id {
            self.update_task_status(&task_id, TaskStatus::Claimed, Some(agent.clone())).await;
        }

        (task_id, agent_id)
    }

    /// Notify the next subscriber in round-robin order
    async fn notify_round_robin(&self, task_type: &str, task: Task) -> Option<String> {
        let subs = self.subscriptions.read().await;
        let subscribers = subs.get(task_type)?;

        if subscribers.is_empty() {
            return None;
        }

        // Get and increment round-robin index
        let mut rr = self.rr_index.write().await;
        let index = rr.entry(task_type.to_string()).or_insert(0);
        let start_index = *index;

        // Try subscribers in round-robin order until one accepts
        let notification = Notification { task };

        for i in 0..subscribers.len() {
            let current_index = (start_index + i) % subscribers.len();
            let subscriber = &subscribers[current_index];

            // Try to send notification (non-blocking check)
            if subscriber.tx.try_send(notification.clone()).is_ok() {
                *index = (current_index + 1) % subscribers.len();
                return Some(subscriber.agent_id.clone());
            }
        }

        // No subscriber could accept the task
        None
    }

    /// Update task status
    async fn update_task_status(&self, task_id: &str, status: TaskStatus, agent_id: Option<String>) {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = status;
            if agent_id.is_some() {
                task.agent_id = agent_id;
            }
            task.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
        }
    }

    /// Mark a task as completed
    pub async fn complete_task(&self, task_id: &str, result: String) -> bool {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = TaskStatus::Completed;
            task.result = Some(result);
            task.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            true
        } else {
            false
        }
    }

    /// Mark a task as failed
    pub async fn fail_task(&self, task_id: &str, error: String) -> bool {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = TaskStatus::Failed;
            task.error = Some(error);
            task.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            true
        } else {
            false
        }
    }

    /// Get task by ID
    pub async fn get_task(&self, task_id: &str) -> Option<Task> {
        let tasks = self.tasks.read().await;
        tasks.get(task_id).cloned()
    }

    /// List tasks by type and/or status
    pub async fn list_tasks(&self, task_type: Option<&str>, status: Option<TaskStatus>) -> Vec<Task> {
        let tasks = self.tasks.read().await;
        tasks.values()
            .filter(|t| {
                let type_match = task_type.map_or(true, |tt| t.task_type == tt);
                let status_match = status.map_or(true, |s| t.status == s);
                type_match && status_match
            })
            .cloned()
            .collect()
    }

    /// Manually claim a pending task
    pub async fn claim_task(&self, task_id: &str, agent_id: &str) -> Option<Task> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            if task.status == TaskStatus::Pending {
                task.status = TaskStatus::Claimed;
                task.agent_id = Some(agent_id.to_string());
                task.updated_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                return Some(task.clone());
            }
        }
        None
    }

    /// Get subscriber count for a task type
    pub async fn subscriber_count(&self, task_type: &str) -> usize {
        let subs = self.subscriptions.read().await;
        subs.get(task_type).map_or(0, |s| s.len())
    }

    /// Get all subscribed task types
    pub async fn subscribed_types(&self) -> Vec<String> {
        let subs = self.subscriptions.read().await;
        subs.keys().cloned().collect()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscribe_unsubscribe() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(10);

        manager.subscribe("research", "agent1", tx.clone()).await;
        assert_eq!(manager.subscriber_count("research").await, 1);

        manager.unsubscribe("research", "agent1").await;
        assert_eq!(manager.subscriber_count("research").await, 0);
    }

    #[tokio::test]
    async fn test_round_robin_distribution() {
        let manager = SubscriptionManager::new();
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        manager.subscribe("test", "agent1", tx1).await;
        manager.subscribe("test", "agent2", tx2).await;

        // Create first task
        let task1 = Task::new("1".to_string(), "test".to_string(), "{}".to_string(), 0);
        let (_, agent1) = manager.create_task(task1).await;

        // Create second task
        let task2 = Task::new("2".to_string(), "test".to_string(), "{}".to_string(), 0);
        let (_, agent2) = manager.create_task(task2).await;

        // Should go to different agents (round-robin)
        assert_ne!(agent1, agent2);

        // Verify notifications were received
        assert!(rx1.try_recv().is_ok() || rx2.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_task_lifecycle() {
        let manager = SubscriptionManager::new();

        let task = Task::new("task1".to_string(), "test".to_string(), "{\"key\":\"value\"}".to_string(), 1000);
        let (task_id, _) = manager.create_task(task).await;

        // Get task
        let retrieved = manager.get_task(&task_id).await.unwrap();
        assert_eq!(retrieved.id, "task1");
        assert_eq!(retrieved.status, TaskStatus::Pending);

        // Complete task
        manager.complete_task(&task_id, "{\"result\":\"done\"}".to_string()).await;

        let completed = manager.get_task(&task_id).await.unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(completed.result, Some("{\"result\":\"done\"}".to_string()));
    }

    #[tokio::test]
    async fn test_claim_task() {
        let manager = SubscriptionManager::new();

        let task = Task::new("task1".to_string(), "test".to_string(), "{}".to_string(), 0);
        manager.create_task(task).await;

        // Claim the task
        let claimed = manager.claim_task("task1", "agent1").await.unwrap();
        assert_eq!(claimed.status, TaskStatus::Claimed);
        assert_eq!(claimed.agent_id, Some("agent1".to_string()));

        // Can't claim already claimed task
        let second_claim = manager.claim_task("task1", "agent2").await;
        assert!(second_claim.is_none());
    }
}
