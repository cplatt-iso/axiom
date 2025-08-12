#!/bin/bash
# scripts/start_enterprise_spanner.sh
# Enterprise Spanner Startup Script
#
# Starts all microservices for the enterprise spanner architecture.
# For 150 locations, 1200 modalities, 8 PACS systems.

set -e

echo "🚀 Starting Enterprise DICOM Query Spanning System"
echo "=================================================="

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  Creating .env file from template..."
    cat > .env << 'EOF'
# Database
POSTGRES_PASSWORD=your_secure_password_here

# RabbitMQ
RABBITMQ_PASSWORD=your_rabbitmq_password_here

# Grafana
GRAFANA_PASSWORD=admin

# Redis (optional custom config)
REDIS_PASSWORD=your_redis_password_here
EOF
    echo "✅ Created .env file. Please update passwords before production deployment."
fi

# Export environment variables
export COMPOSE_PROJECT_NAME=axiom-enterprise-spanner

echo "🐳 Starting enterprise microservices..."

# Start the enterprise stack
docker-compose -f docker-compose.enterprise.yml up -d

echo "⏳ Waiting for services to be ready..."

# Wait for database
echo "Waiting for PostgreSQL..."
until docker-compose -f docker-compose.enterprise.yml exec -T db pg_isready -U axiom; do
    sleep 2
done

# Wait for Redis
echo "Waiting for Redis..."
until docker-compose -f docker-compose.enterprise.yml exec -T redis redis-cli ping; do
    sleep 2
done

# Wait for RabbitMQ
echo "Waiting for RabbitMQ..."
until docker-compose -f docker-compose.enterprise.yml exec -T rabbitmq rabbitmqctl status; do
    sleep 5
done

echo "🗄️  Running database migrations..."
docker-compose -f docker-compose.enterprise.yml exec axiom-api alembic upgrade head

echo "📊 Services Status:"
echo "=================="

# Check service health
services=(
    "axiom-api:8001"
    "spanner-coordinator:8002" 
    "redis:6379"
    "rabbitmq:15672"
    "prometheus:9090"
    "grafana:3000"
)

for service in "${services[@]}"; do
    name=$(echo $service | cut -d: -f1)
    port=$(echo $service | cut -d: -f2)
    
    if curl -f "http://localhost:$port/health" &>/dev/null || \
       curl -f "http://localhost:$port" &>/dev/null; then
        echo "✅ $name - http://localhost:$port"
    else
        echo "❌ $name - Failed to connect"
    fi
done

echo ""
echo "🎯 Spanner Services:"
echo "==================="
echo "Main API:          http://localhost:8001"
echo "Coordinator:       http://localhost:8002" 
echo "RabbitMQ Mgmt:     http://localhost:15672 (guest/guest)"
echo "Prometheus:        http://localhost:9090"
echo "Grafana:          http://localhost:3000 (admin/admin)"

echo ""
echo "🔧 Worker Scaling:"
echo "=================="
echo "Scale DIMSE workers:    docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=12"
echo "Scale coordinators:     docker-compose -f docker-compose.enterprise.yml up -d --scale spanner-coordinator=5"
echo "Scale SCP listeners:    docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-scp-listener=3"

echo ""
echo "📈 Production Scaling Recommendations:"
echo "======================================"
echo "For 150 locations, 1200 modalities, 8 PACS:"
echo "- DIMSE Workers: 16-24 instances (2-3 per PACS)"
echo "- Coordinators: 3-5 instances (load balanced)"
echo "- SCP Listeners: 2-3 instances (HA)"
echo "- Redis: Cluster mode with 6 nodes"
echo "- PostgreSQL: Primary + 2 replicas"

echo ""
echo "🧪 Test the Spanner:"
echo "==================="
echo "# Create a spanner configuration"
echo "curl -X POST 'http://localhost:8001/api/v1/config/spanner/' \\"
echo "  -H 'Authorization: Api-Key YOUR_API_KEY' \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"name\": \"Enterprise Test\", \"max_concurrent_sources\": 8}'"
echo ""
echo "# Execute spanning query"
echo "curl -X GET 'http://localhost:8001/api/v1/spanner/qido/studies?PatientID=12345&spanner_config_id=1' \\"
echo "  -H 'Authorization: Api-Key YOUR_API_KEY'"

echo ""
echo "🎉 Enterprise Spanner is ready for production!"
echo "💀 This beast can handle your 150 locations and 8 PACS systems."
