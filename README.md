# Open Food Facts Static Server

A high-performance Rust-based static server for Open Food Facts data processing. This tool processes the Open Food Facts CSV dataset and generates optimized static files for easy querying without API limits.

## Features

- **High Performance**: Written in Rust with parallel processing for maximum speed
- **Memory Efficient**: Streaming CSV processing with batched operations
- **Docker Support**: Containerized execution for consistent environments
- **Static Output**: Generates static JSON files and indexes for fast querying
- **Progress Tracking**: Real-time progress indicators during processing

## Prerequisites

- **Docker** (recommended) or **Rust 1.75+** for local development
- **curl** (for downloading data)

## Quick Start with Docker

1. **Download the data and run**:
   ```bash
   make docker-all
   ```
   
   Or manually:
   ```bash
   # Download data
   make download
   
   # Run with Docker
   docker-compose up --build
   ```

2. **Access the results**:
   - Products: `static/products/` (individual JSON files)
   - Indexes: `static/indexes/` (categorized and paginated indexes)
   - Catalog: `static/indexes/catalog.jsonl.gz` (compressed catalog)

## Local Development

If you prefer to run locally without Docker:

1. **Install Rust** (if not already installed):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source ~/.cargo/env
   ```

2. **Download data, build, and run**:
   ```bash
   make all
   ```
   
   Or manually:
   ```bash
   # Download data
   make download
   
   # Build
   make build
   
   # Run
   make run
   ```

## Available Commands

Use `make help` to see all available commands:

- `make download` - Download the Open Food Facts data
- `make build` - Build the Rust application
- `make run` - Run the processor
- `make docker-build` - Build Docker image
- `make docker-run` - Run with Docker Compose
- `make docker-all` - Download data and run with Docker
- `make all` - Download data, build, and run locally
- `make clean` - Clean build artifacts

## Project Structure

```
├── src/                    # Rust source code
│   └── main.rs            # Main processing logic
├── static/                # Generated static files
│   ├── products/          # Individual product JSON files
│   └── indexes/           # Categorized indexes and catalog
├── food_facts_raw_data/   # Input CSV data
├── Cargo.toml            # Rust dependencies
├── Dockerfile            # Docker configuration
├── docker-compose.yml    # Docker Compose setup
└── README.md             # This file
```

## Performance

The Rust implementation provides significant performance improvements over the original TypeScript version:

- **Parallel Processing**: Uses Rayon for CPU-intensive operations
- **Memory Efficient**: Streaming CSV processing with configurable batch sizes
- **Optimized I/O**: Concurrent file operations and compressed output
- **Progress Tracking**: Real-time progress indicators

Typical processing times:
- **~2M products**: 2-5 minutes (depending on hardware)
- **Memory usage**: <1GB RAM
- **Output size**: ~500MB compressed

## Configuration

Key configuration options in `src/main.rs`:

- `PAGE_SIZE`: Number of items per index page (default: 500)
- `BATCH_SIZE`: Processing batch size (default: 1000)
- `CSV_SEPARATOR`: CSV delimiter (default: tab)

## Output Format

### Product Files (`static/products/{code}.json`)
```json
{
  "code": "123456789",
  "product_name": "Product Name",
  "brands": "Brand Name",
  "main_category": "Category",
  "macros": {
    "serving_size": "100g",
    "serving_quantity": 100.0,
    "serving_unit": "g",
    "serving": { /* per-serving values */ },
    "per100g": { /* per-100g values */ }
  }
}
```

### Index Files (`static/indexes/{category|brands}/{shard}/{key}/`)
- `_meta.json`: Metadata (count, pages, etc.)
- `page-0001.json`: Paginated results
- `page-0002.json`: Next page, etc.

### Catalog (`static/indexes/catalog.jsonl.gz`)
Compressed JSONL file with all products for full-text search.

## Docker Details

The Docker setup includes:

- **Multi-stage build**: Optimized image size
- **Security**: Non-root user execution
- **Volume mounting**: Direct access to input/output files
- **Error handling**: Clear messages for missing data

## Troubleshooting

### Common Issues

1. **"products.csv.gz not found"**:
   - Ensure you've downloaded the data file
   - Check the file path: `food_facts_raw_data/products.csv.gz`

2. **Permission errors**:
   - On Linux/macOS: `chmod +x target/release/process_data`
   - With Docker: Check volume mount permissions

3. **Out of memory**:
   - Reduce `BATCH_SIZE` in the source code
   - Ensure sufficient disk space for output

### Performance Tuning

- **Increase batch size** for more memory but faster processing
- **Adjust page size** for different index granularity
- **Use SSD storage** for better I/O performance

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with Docker: `docker-compose up --build`
5. Submit a pull request

## License

(ODbL) v1.0 [Open Data Commons Open Database License]()
