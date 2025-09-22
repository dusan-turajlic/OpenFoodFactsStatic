use anyhow::{Context, Result};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{info, warn};
use rustls::{Certificate, PrivateKey, ServerConfig};
use rcgen::{Certificate as RcgenCert, CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
use time::{OffsetDateTime, Duration};

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
            if extension == "br" {
                if let Some(stem) = file_path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if stem_str.ends_with(".jsonl") {
                            return "application/x-ndjson";
                        }
                    }
                }
            }
        }
        
        // Default to JSON for other files
        return "application/json";
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

    fn get_content_encoding(&self, file_path: &Path) -> &'static str {
        if let Some(extension) = file_path.extension() {
            if extension == "gz" {
                return "gzip";
            }
            if extension == "br" {
                return "br";
            }
        }

        return "utf-8";
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

    if method == Method::OPTIONS {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("access-control-allow-origin", "*")
            .header("access-control-allow-methods", "GET, OPTIONS")
            .header("access-control-allow-headers", "*")
            .body(Full::new(Bytes::from("")))
            .unwrap());
    }

    // Only handle GET requests
    if method != Method::GET {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("content-type", "application/json")
            .header("access-control-allow-origin", "*")
            .body(Full::new(Bytes::from(r#"{"error": "Method not allowed"}"#)))
            .unwrap());
    }

    // Handle root path
    if path == "/" || path == "" {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .header("access-control-allow-origin", "*")
            .body(Full::new(Bytes::from(r#"{"message": "OpenFoodFacts Static Server", "endpoints": ["/static/*"]}"#)))
            .unwrap());
    }

    let file_path = state.get_file_path(path);
    let content_type = state.get_content_type(&file_path);
    let content_encoding = state.get_content_encoding(&file_path);

    // Check if file exists
    if !file_path.exists() {
        warn!("❌ File not found: {:?}", file_path);
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "application/json")
            .header("access-control-allow-origin", "*")
            .body(Full::new(Bytes::from(r#"{"error": "File not found"}"#)))
            .unwrap());
    }

    // Check if it's a directory
    if file_path.is_dir() {
        warn!("❌ Path is directory: {:?}", file_path);
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("content-type", "application/json")
            .header("access-control-allow-origin", "*")
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
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
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
    info!("--------------------------------");
    info!("Content encoding: {}", content_encoding);
    info!("Content type: {}", content_type);
    info!("--------------------------------");
    let response_builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_ENCODING, content_encoding)
        .header(header::VARY, "Accept-Encoding")
        .header(header::CONTENT_LENGTH, file_size.to_string());

    Ok(response_builder
        .body(Full::new(Bytes::from(file_contents)))
        .unwrap())
}

fn generate_self_signed_cert() -> Result<(Vec<u8>, Vec<u8>)> {
    let key_pair = KeyPair::generate(&PKCS_ECDSA_P256_SHA256)?;
    
    let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]);
    params.key_pair = Some(key_pair);
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(365); // 1 year
    
    let cert = RcgenCert::from_params(params)?;
    let cert_der = cert.serialize_der()?;
    let key_der = cert.serialize_private_key_der();
    
    Ok((cert_der, key_der))
}

fn load_tls_config() -> Result<Arc<ServerConfig>> {
    let (cert_der, key_der) = generate_self_signed_cert()?;
    
    let cert = Certificate(cert_der);
    let key = PrivateKey(key_der);
    
    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    
    Ok(Arc::new(config))
}

async fn run_server(static_dir: PathBuf) -> Result<()> {
    let state = ServerState::new(static_dir);
    
    let addr = "[::]:8443"; // HTTPS default port
    let listener = TcpListener::bind(addr).await
        .with_context(|| format!("Failed to bind to {}", addr))?;
    
    let tls_config = load_tls_config()?;
    
    info!("🚀 HTTPS Server starting on {}", addr);
    info!("📁 Serving files from: {:?}", state.static_dir);
    info!("🔒 Using self-signed certificate");
    
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        let tls_config = tls_config.clone();
        
        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_request(state.clone(), req));
            
            let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
            
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    
                    if let Err(err) = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        warn!("❌ Error serving connection: {}", err);
                    }
                }
                Err(err) => {
                    warn!("❌ TLS handshake failed: {}", err);
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = std::env::args().collect();
    let static_dir = args.get(1).map(|s| PathBuf::from(s)).unwrap_or_else(|| PathBuf::from("static"));
    
    // Ensure static directory exists
    if !static_dir.exists() {
        fs::create_dir_all(&static_dir)
            .with_context(|| format!("Failed to create static directory: {:?}", static_dir))?;
    }
    
    info!("🎯 OpenFoodFacts Static Server");
    info!("📂 Static directory: {:?}", static_dir);
    
    run_server(static_dir).await?;
    
    Ok(())
}
