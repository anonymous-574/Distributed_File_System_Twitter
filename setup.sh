#!/bin/bash

echo "🚀 Setting up Distributed Social Media System..."

# Create necessary directories
mkdir -p frontend
mkdir -p data/{posts,comments}

# Create the HTML file if it doesn't exist
if [ ! -f "frontend/index.html" ]; then
    echo "Creating frontend files..."
    # The HTML content is already included in the docker-compose.yml above
fi

set -e

echo "✅ Creating Hadoop environment file..."
echo "🐳 Starting Docker containers..."
docker-compose up --build -d

echo "⏳ Waiting for services to start..."
sleep 30

echo "📊 HDFS NameNode: http://localhost:9870"
echo "🌐 Frontend: http://localhost:3000"
echo "🔗 API Gateway: http://localhost:8080"
echo "🖥️  App Servers: 5001, 5002, 5003"