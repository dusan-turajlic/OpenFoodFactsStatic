use anyhow::{Context, Result};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{info, warn};

#[derive(Clone)]
struct ServerState {
    static_dir: PathBuf,
    bandwidth_stats: Arc<std::sync::Mutex<HashMap<String, (u64, u64)>>>, // (bytes, requests)
}

impl ServerState {
    fn new(static_dir: PathBuf) -> Self {
        Self {
            static_dir,
            bandwidth_stats: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn log_bandwidth(&self, file_path: &str, bytes: u64) {
        let mut stats = self.bandwidth_stats.lock().unwrap();
        let entry = stats.entry(file_path.to_string()).or_insert((0, 0));
        entry.0 += bytes;
        entry.1 += 1;
        
        info!(
            "📊 Bandwidth: {} - {} bytes ({} requests total)",
            file_path,
            bytes,
            entry.1
        );
    }

    fn get_content_type(&self, file_path: &Path) -> &'static str {
        if let Some(extension) = file_path.extension() {
            if extension == "gz" {
                if let Some(stem) = file_path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if stem_str.ends_with(".jsonl") {
                            return "application/gzip";
                        }
                    }
                }
            }
        }
        
        // Default to JSON for other files
        "application/json"
    }

    fn get_file_path(&self, request_path: &str) -> PathBuf {
        // Remove leading slash and resolve path
        let clean_path = request_path.trim_start_matches('/');
        let file_path = self.static_dir.join(clean_path);
        
        // Security check: ensure the resolved path is within static directory
        if !file_path.starts_with(&self.static_dir) {
            return self.static_dir.join("404.json");
        }
        
        file_path
    }
}

async fn handle_request(
    state: ServerState,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let start_time = Instant::now();
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();

    info!("🌐 {} {}", method, path);

    // Only handle GET requests
    if method != Method::GET {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"error": "Method not allowed"}"#)))
            .unwrap());
    }

    // Handle root path
    if path == "/" || path == "" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"message": "OpenFoodFacts Static Server", "endpoints": ["/static/*"]}"#)))
            .unwrap());
    }

    let file_path = state.get_file_path(path);
    let content_type = state.get_content_type(&file_path);

    // Check if file exists
    if !file_path.exists() {
        warn!("❌ File not found: {:?}", file_path);
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"error": "File not found"}"#)))
            .unwrap());
    }

    // Check if it's a directory
    if file_path.is_dir() {
        warn!("❌ Path is directory: {:?}", file_path);
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(r#"{"error": "Path is a directory"}"#)))
            .unwrap());
    }

    // Read file
    let file_contents = match fs::read(&file_path) {
        Ok(contents) => contents,
        Err(e) => {
            warn!("❌ Error reading file {:?}: {}", file_path, e);
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(r#"{"error": "Internal server error"}"#)))
                .unwrap());
        }
    };

    let file_size = file_contents.len() as u64;
    let file_name = file_path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    // Log bandwidth usage
    state.log_bandwidth(file_name, file_size);

    let duration = start_time.elapsed();
    info!(
        "✅ Served {} ({} bytes) in {:.2}ms",
        file_name,
        file_size,
        duration.as_secs_f64() * 1000.0
    );

    // Build response
    let mut response_builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .header("content-length", file_size.to_string());

    // For .jsonl.gz files, don't set content-encoding since browser will handle decompression
    if content_type == "application/gzip" {
        response_builder = response_builder.header("content-encoding", "");
    }

    Ok(response_builder
        .body(Full::new(Bytes::from(file_contents)))
        .unwrap())
}

async fn run_server(addr: &str, static_dir: PathBuf) -> Result<()> {
    let state = ServerState::new(static_dir);
    
    let listener = TcpListener::bind(addr).await
        .with_context(|| format!("Failed to bind to {}", addr))?;
    
    info!("🚀 Server starting on {}", addr);
    info!("📁 Serving files from: {:?}", state.static_dir);
    
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        
        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_request(state.clone(), req));
            
            let io = hyper_util::rt::TokioIo::new(stream);
            
            if let Err(err) = Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                warn!("❌ Error serving connection: {}", err);
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:3000");
    let static_dir = args.get(2).map(|s| PathBuf::from(s)).unwrap_or_else(|| PathBuf::from("static"));
    
    // Ensure static directory exists
    if !static_dir.exists() {
        fs::create_dir_all(&static_dir)
            .with_context(|| format!("Failed to create static directory: {:?}", static_dir))?;
    }
    
    info!("🎯 OpenFoodFacts Static Server");
    info!("📍 Address: {}", addr);
    info!("📂 Static directory: {:?}", static_dir);
    
    run_server(addr, static_dir).await?;
    
    Ok(())
}
