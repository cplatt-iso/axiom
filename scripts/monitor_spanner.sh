#!/bin/bash
# scripts/monitor_spanner.sh
"""
Enterprise Spanner Monitoring Script

Monitors the health and performance of all spanner microservices.
Provides real-time stats for enterprise deployments.
"""

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔍 Enterprise DICOM Spanner - Live Monitoring${NC}"
echo "=============================================="
echo "Press Ctrl+C to exit"
echo ""

while true; do
    clear
    echo -e "${BLUE}📊 Enterprise DICOM Spanner Status - $(date)${NC}"
    echo "=============================================="
    
    # Core Services Health
    echo -e "${YELLOW}🏗️  Core Services:${NC}"
    services=(
        "axiom-api:8001:/health"
        "spanner-coordinator:8002:/health"
        "db:5432:"
        "redis:6379:"
        "rabbitmq:15672:"
    )
    
    for service in "${services[@]}"; do
        name=$(echo $service | cut -d: -f1)
        port=$(echo $service | cut -d: -f2)
        endpoint=$(echo $service | cut -d: -f3)
        
        if [ -n "$endpoint" ]; then
            if curl -f -s "http://localhost:$port$endpoint" >/dev/null 2>&1; then
                echo -e "  ✅ $name"
            else
                echo -e "  ❌ $name"
            fi
        else
            # For services without HTTP endpoints
            case $name in
                "db")
                    if docker-compose -f docker-compose.enterprise.yml exec -T db pg_isready -U axiom >/dev/null 2>&1; then
                        echo -e "  ✅ $name"
                    else
                        echo -e "  ❌ $name"
                    fi
                    ;;
                "redis")
                    if docker-compose -f docker-compose.enterprise.yml exec -T redis redis-cli ping >/dev/null 2>&1; then
                        echo -e "  ✅ $name"
                    else
                        echo -e "  ❌ $name"
                    fi
                    ;;
                "rabbitmq")
                    if docker-compose -f docker-compose.enterprise.yml exec -T rabbitmq rabbitmqctl status >/dev/null 2>&1; then
                        echo -e "  ✅ $name"
                    else
                        echo -e "  ❌ $name"
                    fi
                    ;;
            esac
        fi
    done
    
    echo ""
    
    # Worker Status
    echo -e "${YELLOW}⚡ Worker Services:${NC}"
    worker_count=$(docker ps --filter "name=dimse-query-worker" --format "table {{.Names}}" | grep -c dimse-query-worker || echo "0")
    listener_count=$(docker ps --filter "name=dimse-scp-listener" --format "table {{.Names}}" | grep -c dimse-scp-listener || echo "0")
    coordinator_count=$(docker ps --filter "name=spanner-coordinator" --format "table {{.Names}}" | grep -c spanner-coordinator || echo "0")
    
    echo -e "  🔧 DIMSE Workers: $worker_count"
    echo -e "  🎯 Coordinators: $coordinator_count"
    echo -e "  📡 SCP Listeners: $listener_count"
    
    echo ""
    
    # Container Resource Usage
    echo -e "${YELLOW}📈 Resource Usage:${NC}"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" \
        $(docker ps --filter "label=com.docker.compose.project=axiom-enterprise-spanner" --format "{{.Names}}") 2>/dev/null | \
        head -10 | while IFS=$'\t' read -r container cpu mem mempct; do
        if [ "$container" != "CONTAINER" ]; then
            # Color code based on CPU usage
            cpu_val=$(echo $cpu | sed 's/%//')
            if (( $(echo "$cpu_val > 80" | bc -l) )); then
                echo -e "  ${RED}$container: $cpu CPU, $mem RAM${NC}"
            elif (( $(echo "$cpu_val > 50" | bc -l) )); then
                echo -e "  ${YELLOW}$container: $cpu CPU, $mem RAM${NC}"
            else
                echo -e "  ${GREEN}$container: $cpu CPU, $mem RAM${NC}"
            fi
        fi
    done
    
    echo ""
    
    # RabbitMQ Queue Status
    echo -e "${YELLOW}📬 Message Queues:${NC}"
    if docker-compose -f docker-compose.enterprise.yml exec -T rabbitmq rabbitmqctl list_queues 2>/dev/null | grep -E "(spanner|dimse)" | head -5; then
        :
    else
        echo "  No queue data available"
    fi
    
    echo ""
    
    # Recent Query Activity (if spanner coordinator is running)
    echo -e "${YELLOW}🔍 Recent Query Activity:${NC}"
    if curl -f -s "http://localhost:8002/metrics" >/dev/null 2>&1; then
        echo "  📊 Live metrics available at http://localhost:8002/metrics"
    else
        echo "  No coordinator metrics available"
    fi
    
    echo ""
    
    # System Load
    echo -e "${YELLOW}💻 System Load:${NC}"
    echo "  Load: $(uptime | awk -F'load average:' '{ print $2 }')"
    echo "  Memory: $(free -h | awk '/^Mem/ { print $3 "/" $2 " (" $5 " available)" }')"
    echo "  Disk: $(df -h / | awk 'NR==2 { print $3 "/" $2 " (" $5 " used)" }')"
    
    echo ""
    echo -e "${BLUE}🎯 Quick Actions:${NC}"
    echo "  Scale workers:     docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=16"
    echo "  View logs:         docker-compose -f docker-compose.enterprise.yml logs -f"
    echo "  Check queues:      docker-compose -f docker-compose.enterprise.yml exec rabbitmq rabbitmqctl list_queues"
    echo "  Prometheus:        http://localhost:9090"
    echo "  Grafana:          http://localhost:3000"
    
    sleep 10
done
