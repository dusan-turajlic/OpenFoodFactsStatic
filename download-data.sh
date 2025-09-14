#!/bin/bash

# Download Open Food Facts data
echo "📥 Downloading Open Food Facts data..."

# Create directory if it doesn't exist
mkdir -p food_facts_raw_data

# Download the data file
curl -L https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz -o food_facts_raw_data/products.csv.gz

# Check if download was successful
if [ $? -eq 0 ]; then
    echo "✅ Data downloaded successfully!"
    echo "📁 File location: food_facts_raw_data/products.csv.gz"
    echo "📊 File size: $(du -h food_facts_raw_data/products.csv.gz | cut -f1)"
else
    echo "❌ Download failed!"
    exit 1
fi
