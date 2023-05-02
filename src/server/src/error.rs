use crate::auth::AuthError;

/// Error type for server configuration errors
#[derive(Debug)]
pub enum ServerError {
    InvalidPort(String),
    IoError(std::io::Error),
    AuthError(AuthError),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::InvalidPort(msg) => write!(f, "Invalid port: {}", msg),
            ServerError::IoError(e) => write!(f, "IO error: {}", e),
            ServerError::AuthError(e) => write!(f, "Auth error: {}", e),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        ServerError::IoError(err)
    }
}

impl From<AuthError> for ServerError {
    fn from(err: AuthError) -> Self {
        ServerError::AuthError(err)
    }
}
