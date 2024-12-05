use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use log::info;

/// User role for access control
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserRole {
    /// Full access - can manage users and all data operations
    Admin,
    /// Read and write data, but cannot manage users
    ReadWrite,
    /// Read-only access
    ReadOnly,
}

impl UserRole {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(UserRole::Admin),
            "readwrite" | "rw" => Some(UserRole::ReadWrite),
            "readonly" | "ro" => Some(UserRole::ReadOnly),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            UserRole::Admin => "admin",
            UserRole::ReadWrite => "readwrite",
            UserRole::ReadOnly => "readonly",
        }
    }

    /// Check if this role can perform write operations
    pub fn can_write(&self) -> bool {
        matches!(self, UserRole::Admin | UserRole::ReadWrite)
    }

    /// Check if this role can manage users
    pub fn can_manage_users(&self) -> bool {
        matches!(self, UserRole::Admin)
    }
}

/// User information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: UserRole,
}

impl User {
    /// Create a new user with hashed password
    pub fn new(username: String, password: &str, role: UserRole) -> Result<Self, bcrypt::BcryptError> {
        let password_hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)?;
        Ok(User {
            username,
            password_hash,
            role,
        })
    }

    /// Verify password against stored hash
    pub fn verify_password(&self, password: &str) -> bool {
        bcrypt::verify(password, &self.password_hash).unwrap_or(false)
    }

    /// Update password
    pub fn set_password(&mut self, password: &str) -> Result<(), bcrypt::BcryptError> {
        self.password_hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)?;
        Ok(())
    }
}

/// User store for authentication
#[derive(Debug, Serialize, Deserialize)]
struct UserStoreData {
    users: HashMap<String, User>,
}

/// User management system
pub struct UserStore {
    users: Arc<RwLock<HashMap<String, User>>>,
    file_path: Option<String>,
}

impl UserStore {
    /// Create a new user store (in-memory only)
    pub fn new() -> Self {
        UserStore {
            users: Arc::new(RwLock::new(HashMap::new())),
            file_path: None,
        }
    }

    /// Create a user store with persistence
    pub fn with_persistence<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_path = path.as_ref().to_string_lossy().to_string();
        let users = if path.as_ref().exists() {
            let content = std::fs::read_to_string(&path)?;
            let data: UserStoreData = serde_json::from_str(&content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            info!("Loaded {} users from {}", data.users.len(), file_path);
            data.users
        } else {
            HashMap::new()
        };

        Ok(UserStore {
            users: Arc::new(RwLock::new(users)),
            file_path: Some(file_path),
        })
    }

    /// Save users to disk (if persistence is enabled)
    async fn save(&self) -> std::io::Result<()> {
        if let Some(ref path) = self.file_path {
            let users = self.users.read().await;
            let data = UserStoreData {
                users: users.clone(),
            };
            let content = serde_json::to_string_pretty(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            tokio::fs::write(path, content).await?;
        }
        Ok(())
    }

    /// Add a new user
    pub async fn add_user(&self, username: &str, password: &str, role: UserRole) -> Result<(), AuthError> {
        let mut users = self.users.write().await;

        if users.contains_key(username) {
            return Err(AuthError::UserExists);
        }

        let user = User::new(username.to_string(), password, role)
            .map_err(|_| AuthError::InternalError)?;

        users.insert(username.to_string(), user);
        drop(users);

        self.save().await.map_err(|_| AuthError::InternalError)?;
        info!("User '{}' created", username);
        Ok(())
    }

    /// Remove a user
    pub async fn remove_user(&self, username: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().await;

        if users.remove(username).is_none() {
            return Err(AuthError::UserNotFound);
        }

        drop(users);
        self.save().await.map_err(|_| AuthError::InternalError)?;
        info!("User '{}' removed", username);
        Ok(())
    }

    /// Authenticate user with username and password
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<User, AuthError> {
        let users = self.users.read().await;

        let user = users.get(username).ok_or(AuthError::InvalidCredentials)?;

        if user.verify_password(password) {
            Ok(user.clone())
        } else {
            Err(AuthError::InvalidCredentials)
        }
    }

    /// Change user password
    pub async fn change_password(&self, username: &str, new_password: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().await;

        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.set_password(new_password).map_err(|_| AuthError::InternalError)?;

        drop(users);
        self.save().await.map_err(|_| AuthError::InternalError)?;
        info!("Password changed for user '{}'", username);
        Ok(())
    }

    /// Change user role
    pub async fn change_role(&self, username: &str, role: UserRole) -> Result<(), AuthError> {
        let mut users = self.users.write().await;

        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.role = role;

        drop(users);
        self.save().await.map_err(|_| AuthError::InternalError)?;
        info!("Role changed for user '{}'", username);
        Ok(())
    }

    /// List all users (without password hashes)
    pub async fn list_users(&self) -> Vec<(String, UserRole)> {
        let users = self.users.read().await;
        users.iter()
            .map(|(name, user)| (name.clone(), user.role.clone()))
            .collect()
    }

    /// Check if any users exist
    pub async fn has_users(&self) -> bool {
        let users = self.users.read().await;
        !users.is_empty()
    }

    /// Get user count
    pub async fn user_count(&self) -> usize {
        let users = self.users.read().await;
        users.len()
    }

    /// Check if authentication is enabled (has users)
    pub async fn is_enabled(&self) -> bool {
        self.has_users().await
    }

    /// Create default admin user if no users exist
    pub async fn ensure_default_admin(&self, default_password: &str) -> Result<bool, AuthError> {
        if self.has_users().await {
            return Ok(false);
        }

        self.add_user("admin", default_password, UserRole::Admin).await?;
        Ok(true)
    }
}

impl Clone for UserStore {
    fn clone(&self) -> Self {
        UserStore {
            users: Arc::clone(&self.users),
            file_path: self.file_path.clone(),
        }
    }
}

/// Authentication errors
#[derive(Debug, Clone, PartialEq)]
pub enum AuthError {
    InvalidCredentials,
    UserExists,
    UserNotFound,
    PermissionDenied,
    NotAuthenticated,
    InternalError,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidCredentials => write!(f, "Invalid username or password"),
            AuthError::UserExists => write!(f, "User already exists"),
            AuthError::UserNotFound => write!(f, "User not found"),
            AuthError::PermissionDenied => write!(f, "Permission denied"),
            AuthError::NotAuthenticated => write!(f, "Authentication required"),
            AuthError::InternalError => write!(f, "Internal error"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Session state for authenticated connections
#[derive(Debug, Clone)]
pub struct Session {
    pub user: Option<User>,
}

impl Session {
    pub fn new() -> Self {
        Session { user: None }
    }

    pub fn authenticate(&mut self, user: User) {
        self.user = Some(user);
    }

    pub fn is_authenticated(&self) -> bool {
        self.user.is_some()
    }

    pub fn can_write(&self) -> bool {
        self.user.as_ref().map(|u| u.role.can_write()).unwrap_or(false)
    }

    pub fn can_manage_users(&self) -> bool {
        self.user.as_ref().map(|u| u.role.can_manage_users()).unwrap_or(false)
    }

    pub fn username(&self) -> Option<&str> {
        self.user.as_ref().map(|u| u.username.as_str())
    }

    pub fn role(&self) -> Option<&UserRole> {
        self.user.as_ref().map(|u| &u.role)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_user_creation() {
        let store = UserStore::new();
        store.add_user("test", "password123", UserRole::ReadWrite).await.unwrap();

        let user = store.authenticate("test", "password123").await.unwrap();
        assert_eq!(user.username, "test");
        assert_eq!(user.role, UserRole::ReadWrite);
    }

    #[tokio::test]
    async fn test_invalid_password() {
        let store = UserStore::new();
        store.add_user("test", "password123", UserRole::ReadWrite).await.unwrap();

        let result = store.authenticate("test", "wrongpassword").await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_user_not_found() {
        let store = UserStore::new();

        let result = store.authenticate("nonexistent", "password").await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_duplicate_user() {
        let store = UserStore::new();
        store.add_user("test", "password123", UserRole::ReadWrite).await.unwrap();

        let result = store.add_user("test", "different", UserRole::Admin).await;
        assert!(matches!(result, Err(AuthError::UserExists)));
    }

    #[tokio::test]
    async fn test_password_change() {
        let store = UserStore::new();
        store.add_user("test", "oldpassword", UserRole::ReadWrite).await.unwrap();

        store.change_password("test", "newpassword").await.unwrap();

        // Old password should fail
        let result = store.authenticate("test", "oldpassword").await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));

        // New password should work
        store.authenticate("test", "newpassword").await.unwrap();
    }
}
