# Enterprise DICOM Spanning - Quick Start Guide

## Overview

This quick start guide will get your enterprise DICOM query spanning system up and running in minutes.

## Prerequisites

✅ **Working Axiom DICOM System**: Your existing docker-compose.yml should be operational  
✅ **Docker & Docker Compose**: Version 20.10+ recommended  
✅ **Network Access**: Connectivity to your PACS systems  
✅ **Logging Infrastructure**: Fluentd running on 127.0.0.1:24224  

## 5-Minute Setup

### Step 1: Deploy Enterprise Services

```bash
cd axiom/backend
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build
```

### Step 2: Verify Services

```bash
# Check all services are running
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps

# Test health endpoints
curl http://localhost:8001/health  # Original API
curl http://localhost:8002/health  # Enterprise Coordinator
```

Expected response:
```json
{"status": "healthy", "service": "spanner-coordinator"}
```

### Step 3: Configure Your First PACS

```bash
# Create a spanner configuration
curl -X POST "http://localhost:8001/spanner/configs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Hospital Network",
    "description": "Spanning across hospital PACS systems",
    "query_timeout_seconds": 300,
    "max_concurrent_queries": 10,
    "failure_strategy": "continue_on_partial_failure",
    "deduplication_strategy": "by_study_instance_uid",
    "priority": 1,
    "is_active": true
  }'
```

### Step 4: Add Your PACS Systems

```bash
# Add your first PACS system
curl -X POST "http://localhost:8001/spanner/sources" \
  -H "Content-Type: application/json" \
  -d '{
    "spanner_config_id": 1,
    "name": "Main Hospital PACS",
    "source_type": "dimse",
    "connection_config": {
      "ae_title": "MAIN_PACS",
      "host": "YOUR_PACS_IP",
      "port": 104,
      "calling_ae_title": "AXIOM_SPANNER"
    },
    "query_config": {
      "max_results_per_query": 1000,
      "timeout_seconds": 60
    },
    "is_active": true,
    "priority": 1
  }'
```

**Replace `YOUR_PACS_IP` with your actual PACS IP address.**

### Step 5: Test Your First Spanning Query

```bash
# Create a test query
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY" \
  -H "Content-Type: application/json" \
  -d '{
    "PatientID": "*",
    "StudyDate": "20240101-"
  }'
```

Response:
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "message": "Spanning query accepted for processing"
}
```

### Step 6: Check Query Results

```bash
# Check query status (replace with your query_id)
curl "http://localhost:8002/spanning-query/550e8400-e29b-41d4-a716-446655440000"
```

## Service Overview

### Running Services

| Service | Port | Purpose |
|---------|------|---------|
| **Original API** | 8001 | Main DICOM processor + Spanner management |
| **Enterprise Coordinator** | 8002 | Spanning query orchestration |
| **DIMSE Listeners** | 11112-11114 | DICOM SCP endpoints |
| **MLLP Listener** | 2575 | HL7 message processing |

### Key Features Available

✅ **Multi-PACS Querying**: Query across all your PACS systems simultaneously  
✅ **Real-time Status**: Track query progress and results  
✅ **Fault Tolerance**: Continues even if some PACS systems are unavailable  
✅ **Result Deduplication**: Intelligent handling of duplicate studies  
✅ **Scalable Workers**: Automatically scales for high query volumes  
✅ **Comprehensive Logging**: All activity logged to your ELK stack  

## Common Tasks

### Add More PACS Systems

For each additional PACS system:

```bash
curl -X POST "http://localhost:8001/spanner/sources" \
  -H "Content-Type: application/json" \
  -d '{
    "spanner_config_id": 1,
    "name": "Emergency Dept PACS",
    "source_type": "dimse",
    "connection_config": {
      "ae_title": "ED_PACS",
      "host": "192.168.2.100",
      "port": 104,
      "calling_ae_title": "AXIOM_SPANNER"
    },
    "query_config": {
      "max_results_per_query": 1000,
      "timeout_seconds": 60
    },
    "is_active": true,
    "priority": 2
  }'
```

### Scale Workers for High Volume

```bash
# Scale to 5 workers for better performance
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=5
```

### Monitor System Health

```bash
# Check service status
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps

# View coordinator logs
docker logs -f axiom-spanner-coordinator

# View worker logs
docker logs -f backend-dimse-query-worker-1

# Check queue status
docker exec axiom-rabbitmq rabbitmqctl list_queues
```

## Example Queries

### Patient Search
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=PATIENT" \
  -H "Content-Type: application/json" \
  -d '{
    "PatientID": "12345"
  }'
```

### Study Search by Modality
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY" \
  -H "Content-Type: application/json" \
  -d '{
    "Modality": "CT",
    "StudyDate": "20240801-"
  }'
```

### Search by Study Description
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY" \
  -H "Content-Type: application/json" \
  -d '{
    "StudyDescription": "*CHEST*",
    "StudyDate": "20240101-20241231"
  }'
```

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker logs axiom-spanner-coordinator
docker logs backend-dimse-query-worker-1

# Common fixes:
# 1. Ensure original system is running first
docker compose up -d

# 2. Check database connectivity
docker exec axiom-db pg_isready

# 3. Restart services
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml restart
```

### Query Failures

```bash
# Check query status for error details
curl "http://localhost:8002/spanning-query/{query_id}"

# Common issues:
# - PACS connectivity (check network/firewall)
# - Invalid AE titles (verify PACS configuration)
# - Query timeout (increase timeout in source config)
```

### Performance Issues

```bash
# Scale workers
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=10

# Check resource usage
docker stats

# Monitor queue depth
docker exec axiom-rabbitmq rabbitmqctl list_queues
```

## Next Steps

### Production Deployment

1. **Security**: Configure proper authentication and SSL certificates
2. **Monitoring**: Set up alerts in your ELK stack for failures
3. **Backup**: Ensure database backup includes spanner configurations
4. **Testing**: Run load tests with realistic query volumes

### Advanced Configuration

1. **Multiple Coordinators**: Deploy additional coordinators for high availability
2. **Geographic Distribution**: Deploy workers closer to remote PACS systems
3. **Priority Queues**: Configure different priorities for urgent vs routine queries
4. **Custom Workflows**: Integrate with your existing DICOM workflow systems

### Scaling for Enterprise

For **150 locations** and **1200 modalities**:

```bash
# Recommended scaling
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d \
  --scale dimse-query-worker=20 \
  --scale spanner-coordinator=3
```

## Support

- **Documentation**: `/docs/ENTERPRISE_SPANNING_GUIDE.md`
- **API Reference**: `/docs/API_REFERENCE.md`
- **Logs**: Check Kibana dashboard for system health
- **Configuration**: All settings via REST API on port 8001

**🎉 You now have enterprise-grade DICOM query spanning across your entire network!**
