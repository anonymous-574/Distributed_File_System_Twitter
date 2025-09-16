#!/bin/bash

echo "🚀 Setting up Distributed Social Media System..."
echo "================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "\n${BLUE}📌 $1${NC}"
}

# Cleanup function
cleanup() {
    print_step "Cleaning up previous containers..."
    docker-compose down -v
    docker system prune -f
}

# Check if cleanup is requested
if [ "$1" = "clean" ]; then
    cleanup
    exit 0
fi

set -e

print_step "Creating necessary directories"
mkdir -p frontend hadoop-config data/{posts,comments}

print_step "Building and starting Docker containers"
docker-compose up --build -d

print_step "Waiting for services to initialize..."

# Function to check if a service is healthy
check_service() {
    local service_name=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo "Checking $service_name..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            print_status "$service_name is healthy! ✅"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    print_error "$service_name failed to start properly ❌"
    return 1
}

# Wait for namenode
print_step "Waiting for Hadoop NameNode to start"
sleep 30
check_service "NameNode Web UI" "http://localhost:9870"

# Wait for app servers
print_step "Waiting for application servers to start"
sleep 20
check_service "App Server 1" "http://localhost:5001/health"
check_service "App Server 2" "http://localhost:5002/health"
check_service "App Server 3" "http://localhost:5003/health"

# Wait for gateway
print_step "Waiting for API Gateway to start"
check_service "API Gateway" "http://localhost:8080/health"

# Wait for frontend
print_step "Waiting for Frontend to start"
check_service "Frontend" "http://localhost:3000"

print_step "System Status Check"
echo "================================================="
print_status "📊 HDFS NameNode Web UI: http://localhost:9870"
print_status "🌐 Frontend Application: http://localhost:3000"
print_status "🔗 API Gateway: http://localhost:8080"
print_status "🖥️  App Server 1: http://localhost:5001"
print_status "🖥️  App Server 2: http://localhost:5002"
print_status "🖥️  App Server 3: http://localhost:5003"
echo "================================================="

print_step "Showing real-time logs (press Ctrl+C to exit)"
echo "To see logs for specific services, use:"
echo "  docker logs -f namenode"
echo "  docker logs -f app_server1"
echo "  docker logs -f app_server2"
echo "  docker logs -f app_server3"
echo "  docker logs -f gateway"
echo ""
echo "Starting combined log view..."
sleep 5

# Show logs from all services
docker-compose logs -f --tail=50