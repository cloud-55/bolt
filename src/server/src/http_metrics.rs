//! HTTP server for Prometheus metrics and health checks.
//!
//! Exposes:
//! - GET /metrics - Prometheus-format metrics
//! - GET /health - Health check endpoint
//! - GET /ready - Readiness check endpoint

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Method};
use hyper_util::rt::TokioIo;
use log::{info, error, debug};
use tokio::net::TcpListener;
use storage::Storage;
use cluster::ClusterManager;

use crate::metrics::Metrics;

/// HTTP metrics server configuration
pub struct HttpMetricsServer {
    storage: Arc<Storage>,
    metrics: Arc<Metrics>,
    cluster: Option<Arc<ClusterManager>>,
    port: u16,
}

impl HttpMetricsServer {
    pub fn new(
        storage: Arc<Storage>,
        metrics: Arc<Metrics>,
        cluster: Option<Arc<ClusterManager>>,
        port: u16,
    ) -> Self {
        Self {
            storage,
            metrics,
            cluster,
            port,
        }
    }

    /// Start the HTTP metrics server
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr).await?;

        info!("HTTP metrics server listening on http://0.0.0.0:{}", self.port);
        info!("  - GET /metrics  - Prometheus metrics");
        info!("  - GET /health   - Health check");
        info!("  - GET /ready    - Readiness check");

        let storage = self.storage;
        let metrics = self.metrics;
        let cluster = self.cluster;

        loop {
            let (stream, remote_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to accept HTTP connection: {}", e);
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let storage = storage.clone();
            let metrics = metrics.clone();
            let cluster = cluster.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let storage = storage.clone();
                    let metrics = metrics.clone();
                    let cluster = cluster.clone();
                    async move {
                        handle_request(req, storage, metrics, cluster).await
                    }
                });

                if let Err(e) = http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    debug!("HTTP connection error from {}: {}", remote_addr, e);
                }
            });
        }
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    storage: Arc<Storage>,
    metrics: Arc<Metrics>,
    cluster: Option<Arc<ClusterManager>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let prometheus_metrics = metrics.to_prometheus(&storage, cluster.as_ref()).await;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(prometheus_metrics)))
                .unwrap()
        }
        (&Method::GET, "/health") => {
            // Basic health check - server is running
            let body = r#"{"status":"healthy"}"#;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        (&Method::GET, "/ready") => {
            // Readiness check - server is ready to accept requests
            let body = r#"{"status":"ready"}"#;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        (&Method::GET, "/") => {
            // Root endpoint with available endpoints
            let body = r#"<!DOCTYPE html>
<html>
<head><title>Bolt Metrics</title></head>
<body>
<h1>Bolt Metrics Server</h1>
<ul>
<li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
<li><a href="/health">/health</a> - Health check</li>
<li><a href="/ready">/ready</a> - Readiness check</li>
</ul>
</body>
</html>"#;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(r#"{"error":"not found"}"#)))
                .unwrap()
        }
    };

    Ok(response)
}
