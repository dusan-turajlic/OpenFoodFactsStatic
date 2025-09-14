.PHONY: help download build run docker-build docker-run clean

help: ## Show this help message
	@echo "Open Food Facts Static Processor"
	@echo "================================"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

download: ## Download the Open Food Facts data
	@echo "📥 Downloading Open Food Facts data..."
	@mkdir -p food_facts_raw_data
	@curl -L https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz -o food_facts_raw_data/products.csv.gz
	@echo "✅ Data downloaded successfully!"

build: ## Build the Rust application
	@echo "🔨 Building Rust application..."
	@cargo build --release
	@echo "✅ Build complete!"

run: ## Run the processor (requires data to be downloaded)
	@echo "🚀 Running processor..."
	@./target/release/process_data

docker-build: ## Build Docker image
	@echo "🐳 Building Docker image..."
	@docker build -t open-food-facts-processor .
	@echo "✅ Docker image built!"

docker-run: ## Run with Docker Compose
	@echo "🐳 Running with Docker Compose..."
	@docker-compose up --build

clean: ## Clean build artifacts
	@echo "🧹 Cleaning build artifacts..."
	@cargo clean
	@rm -rf target/
	@echo "✅ Clean complete!"

all: download build run ## Download data, build, and run

docker-all: download docker-run ## Download data and run with Docker
