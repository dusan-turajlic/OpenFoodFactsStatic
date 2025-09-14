# OpenFoodFacts Static Server

A simple and fast Rust HTTP server that serves static files from a directory with bandwidth logging and proper content-type handling.

## Features

- **Static File Serving**: Serves files from the `static/` directory
- **Bandwidth Logging**: Logs bandwidth usage for each file served
- **Content-Type Handling**: 
  - Default: `application/json` for most files
  - Special: `application/gzip` for `.jsonl.gz` files (no content-encoding header for browser decompression)
- **Security**: Path traversal protection
- **Performance**: Built with Hyper and Tokio for high performance

## Usage

### Build the Server

```bash
cargo build --bin server
```

### Run the Server

```bash
# Default: serve on 127.0.0.1:3000 from ./static directory
cargo run --bin server

# Custom address and directory
cargo run --bin server -- 0.0.0.0:8080 /path/to/static/files
```

### Examples

```bash
# Serve on localhost:3000
cargo run --bin server

# Serve on all interfaces port 8080
cargo run --bin server -- 0.0.0.0:8080

# Serve from custom directory
cargo run --bin server -- 127.0.0.1:3000 /path/to/your/static/files
```

## API Endpoints

- `GET /` - Server info and available endpoints
- `GET /{path}` - Serve static files from the static directory

## Content-Type Rules

- **`.jsonl.gz` files**: `application/gzip` (no content-encoding header)
- **All other files**: `application/json`

## Bandwidth Logging

The server logs bandwidth usage for each file served:

```
📊 Bandwidth: filename.json - 1024 bytes (5 requests total)
✅ Served filename.json (1024 bytes) in 2.34ms
```

## Security Features

- Path traversal protection
- Only serves files within the configured static directory
- Returns 404 for non-existent files
- Returns 400 for directory requests

## Example Response

```json
{
  "message": "OpenFoodFacts Static Server",
  "endpoints": ["/static/*"]
}
```

## Testing

Test the server with:

```bash
# Start the server
cargo run --bin server

# In another terminal, test with curl
curl http://localhost:3000/
curl http://localhost:3000/test.json
```

## Dependencies

- `hyper` - HTTP server
- `tokio` - Async runtime
- `tracing` - Logging
- `anyhow` - Error handling
