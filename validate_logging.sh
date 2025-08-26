#!/bin/bash
# validate_logging.sh - Script to validate JSON logging across all containers

echo "=== Axiom Logging Validation Report ==="
echo ""

# Function to check if a container is running and output logs
check_container_logging() {
    local container_name="$1"
    local description="$2"
    
    echo "--- Checking $description ($container_name) ---"
    
    if docker ps --format "table {{.Names}}" | grep -q "^$container_name$"; then
        echo "✅ Container is running"
        echo "Recent logs (last 5 lines):"
        docker logs --tail 5 "$container_name" 2>&1 | while IFS= read -r line; do
            echo "  $line"
        done
        echo ""
        
        # Try to parse as JSON to validate format
        echo "JSON validation:"
        docker logs --tail 5 "$container_name" 2>&1 | while IFS= read -r line; do
            if echo "$line" | jq . >/dev/null 2>&1; then
                echo "  ✅ JSON: $line"
            else
                echo "  ❌ NOT JSON: $line"
            fi
        done
    else
        echo "❌ Container not running"
    fi
    echo ""
}

echo "Checking all Axiom containers for JSON logging compliance..."
echo ""

# Check main application containers
check_container_logging "dicom_processor_api" "FastAPI Application"
check_container_logging "dicom_processor_worker" "Celery Worker"
check_container_logging "dicom_processor_beat" "Celery Beat Scheduler"

# Check listener containers
check_container_logging "dicom_processor_storescp_1" "DIMSE Store SCP Listener"

# Check worker containers
check_container_logging "dicom_processor_dustbin_verification" "Dustbin Verification Worker"

# Check sender containers (if running)
check_container_logging "dicom_processor_dcm4che_sender" "DCM4CHE Sender"

# Check infrastructure containers (these may not be JSON but should be noted)
echo "--- Infrastructure Containers (May not be JSON) ---"
check_container_logging "dicom_processor_db" "PostgreSQL Database"
check_container_logging "dicom_processor_rabbitmq" "RabbitMQ Message Broker"
check_container_logging "dicom_processor_redis" "Redis Cache"

echo ""
echo "=== Summary ==="
echo "All application containers (API, Worker, Beat, Listeners, Senders) should output JSON logs."
echo "Infrastructure containers (DB, RabbitMQ, Redis) may use their native log formats but are sent to fluentd."
echo ""
echo "To restart containers and apply logging changes:"
echo "  ./axiomctl down"
echo "  ./axiomctl up -d"
echo ""
