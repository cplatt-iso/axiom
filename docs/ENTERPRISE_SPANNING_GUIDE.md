# Enterprise DICOM Query Spanning System

## Overview

This document describes the enterprise-grade DICOM query spanning system built for Axiom DICOM Processor. The system enables querying across multiple PACS systems simultaneously, supporting 150+ locations, 1200+ modalities, and 8 different PACS systems.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [System Components](#system-components)
3. [Deployment Guide](#deployment-guide)
4. [API Documentation](#api-documentation)
5. [Configuration](#configuration)
6. [Monitoring & Logging](#monitoring--logging)
7. [Scaling Guide](#scaling-guide)
8. [Troubleshooting](#troubleshooting)

## Architecture Overview

The enterprise spanning system uses a microservices architecture with the following design principles:

- **Non-disruptive**: Added as overlay to existing system
- **Scalable**: Horizontal scaling for workers and coordinators
- **Fault-tolerant**: Message queues, Redis caching, health monitoring
- **Observable**: Comprehensive logging to existing ELK stack

### System Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Original API  │    │ Spanner Coord.  │    │  DIMSE Workers  │
│   (Port 8001)   │    │   (Port 8002)   │    │   (Scalable)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
    │   PostgreSQL    │    │   RabbitMQ      │    │     Redis       │
    │   (Database)    │    │ (Msg Queue)     │    │   (Cache)       │
    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
    │  DIMSE Listener │    │  MLLP Listener  │    │  Original DIMSE │
    │   (Port 11114)  │    │   (Port 2575)   │    │ (Ports 11112-3) │
    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## System Components

### Core Services

#### 1. **Spanner Coordinator** (Port 8002)
- **Purpose**: Orchestrates spanning queries across multiple PACS
- **Technology**: FastAPI microservice
- **Key Features**:
  - Query distribution and coordination
  - Result aggregation and deduplication
  - Timeout and failure handling
  - Real-time status tracking

#### 2. **DIMSE Query Workers** (Scalable)
- **Purpose**: Execute distributed DICOM queries
- **Technology**: Celery workers with pynetdicom
- **Key Features**:
  - C-FIND, C-MOVE, C-GET support
  - Parallel query execution
  - Fault tolerance and retries
  - Performance monitoring

#### 3. **DIMSE SCP Listener** (Port 11114)
- **Purpose**: Accept external spanning queries
- **Technology**: pynetdicom SCP service
- **Key Features**:
  - Additional DICOM endpoint
  - Integration with coordinator
  - Standard DICOM protocol support

### Supporting Infrastructure

#### Database Schema
- **spanner_configs**: Master configuration for spanning setups
- **spanner_sources**: Individual PACS/source definitions
- **spanner_query_logs**: Query execution history and metrics

#### Message Queues
- **spanning_queries**: Query distribution queue
- **dimse_tasks**: DIMSE-specific task queue
- **results_aggregation**: Result collection queue

#### Caching Layer
- **Query Results**: Temporary result storage
- **Source Status**: PACS availability tracking
- **Performance Metrics**: Response time caching

## Deployment Guide

### Prerequisites

1. **Working Axiom System**: Original docker-compose.yml must be operational
2. **External Dependencies**:
   - Fluentd logging agent (127.0.0.1:24224)
   - ELK stack for log aggregation
   - Network access to target PACS systems

### Quick Start

1. **Deploy Enterprise Services**:
   ```bash
   cd axiom/backend
   docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build
   ```

2. **Verify Services**:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps
   ```

3. **Test Health**:
   ```bash
   curl http://localhost:8001/health  # Original API
   curl http://localhost:8002/health  # Spanner Coordinator
   ```

### Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| Original API | 8001 | Main DICOM processor API |
| Spanner Coordinator | 8002 | Enterprise spanning coordination |
| DIMSE Listeners | 11112-11114 | DICOM SCP endpoints |
| MLLP Listener | 2575 | HL7 message processing |
| PostgreSQL | 5432 | Database access |
| RabbitMQ Management | 15672 | Message queue management |

## API Documentation

### Spanner Coordinator API (Port 8002)

#### Health Check
```bash
GET /health
```
Response:
```json
{
  "status": "healthy",
  "service": "spanner-coordinator"
}
```

#### Create Spanning Query
```bash
POST /spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY
Content-Type: application/json

{
  "PatientID": "12345",
  "StudyDate": "20240101-20241231",
  "StudyDescription": "*MRI*"
}
```

Response:
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "message": "Spanning query accepted for processing"
}
```

#### Query Status
```bash
GET /spanning-query/{query_id}
```

Response:
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "total_results": 150,
  "sources_responded": 8,
  "execution_time_ms": 2500,
  "results": [...]
}
```

### Original API (Port 8001)

#### Spanner Configuration Management
```bash
GET /spanner/configs          # List all configurations
POST /spanner/configs         # Create new configuration
GET /spanner/configs/{id}     # Get specific configuration
PUT /spanner/configs/{id}     # Update configuration
DELETE /spanner/configs/{id}  # Delete configuration
```

#### Source Management
```bash
GET /spanner/sources          # List all sources
POST /spanner/sources         # Create new source
GET /spanner/sources/{id}     # Get specific source
PUT /spanner/sources/{id}     # Update source
DELETE /spanner/sources/{id}  # Delete source
```

## Configuration

### Spanner Configuration Example

```json
{
  "name": "Hospital Network Spanning",
  "description": "Query across all hospital PACS systems",
  "query_timeout_seconds": 300,
  "max_concurrent_queries": 10,
  "failure_strategy": "continue_on_partial_failure",
  "deduplication_strategy": "by_study_instance_uid",
  "priority": 1,
  "is_active": true,
  "sources": [
    {
      "name": "Main Hospital PACS",
      "source_type": "dimse",
      "connection_config": {
        "ae_title": "MAIN_PACS",
        "host": "192.168.1.100",
        "port": 104,
        "calling_ae_title": "AXIOM_SPANNER"
      },
      "query_config": {
        "max_results_per_query": 1000,
        "timeout_seconds": 60
      }
    }
  ]
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `REDIS_URL` | Redis connection string | Required |
| `RABBITMQ_URL` | RabbitMQ connection string | Required |
| `WORKER_TYPE` | Worker type for scaling | `dimse-query` |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP service account key | Optional |

## Monitoring & Logging

### Log Aggregation

All services send logs to your existing ELK stack via Fluentd:

- **Fluentd Address**: 127.0.0.1:24224
- **Log Tags**:
  - `axiom.spanner_coordinator.*`
  - `axiom.dimse_query_worker.*`
  - `axiom.dimse_scp_listener.*`

### Health Monitoring

#### Service Health Checks
```bash
# Check all services
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps

# Check specific service logs
docker logs axiom-spanner-coordinator
docker logs backend-dimse-query-worker-1
```

#### Performance Metrics

The system tracks:
- Query execution times
- Success/failure rates
- Source response times
- Queue depths
- Worker utilization

### Dashboard Queries (Kibana)

```
# Query performance tracking
index: "logstash-*"
message: "spanning_query_completed"

# Error monitoring
index: "logstash-*"
level: "ERROR"
tag: "axiom.*"

# Worker utilization
index: "logstash-*"
message: "dimse_worker_status"
```

## Scaling Guide

### Horizontal Scaling

#### Scale DIMSE Query Workers
```bash
# Scale to 5 workers
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=5
```

#### Add Multiple Coordinators
```yaml
# In docker-compose.enterprise.yml
spanner-coordinator-2:
  extends: spanner-coordinator
  container_name: axiom-spanner-coordinator-2
  ports:
    - "8003:8000"
```

### Performance Optimization

#### Database Optimization
```sql
-- Index optimization for large-scale queries
CREATE INDEX CONCURRENTLY idx_spanner_query_logs_created_at ON spanner_query_logs(created_at);
CREATE INDEX CONCURRENTLY idx_spanner_sources_source_type ON spanner_sources(source_type);
```

#### Redis Configuration
```bash
# Increase memory limit for large result sets
docker exec dicom_processor_redis redis-cli CONFIG SET maxmemory 2gb
docker exec dicom_processor_redis redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

#### RabbitMQ Optimization
```bash
# Increase queue limits for high throughput
docker exec dicom_processor_rabbitmq rabbitmqctl set_policy spanning_policy "spanning_.*" \
  '{"max-length":10000,"overflow":"reject-publish"}'
```

### Enterprise Scale Configuration

For 150 locations and 1200 modalities:

```yaml
# Recommended scaling
services:
  dimse-query-worker:
    deploy:
      replicas: 20  # 20 workers for parallel processing
      
  spanner-coordinator:
    deploy:
      replicas: 3   # 3 coordinators for load balancing
      
  # Additional Redis instances for caching
  redis-cache-1:
    image: redis:7-alpine
    
  redis-cache-2:
    image: redis:7-alpine
```

## Troubleshooting

### Common Issues

#### 1. Services Not Starting
```bash
# Check logs
docker logs axiom-spanner-coordinator
docker logs backend-dimse-query-worker-1

# Common causes:
# - Database connection issues
# - Missing environment variables
# - Port conflicts
```

#### 2. Query Failures
```bash
# Check coordinator logs
curl http://localhost:8002/spanning-query/{query_id}

# Common causes:
# - PACS connectivity issues
# - Invalid DICOM parameters
# - Timeout configuration
```

#### 3. Performance Issues
```bash
# Check queue depth
docker exec dicom_processor_rabbitmq rabbitmqctl list_queues

# Check Redis memory usage
docker exec dicom_processor_redis redis-cli INFO memory

# Check worker status
docker stats backend-dimse-query-worker-1
```

### Log Analysis

#### Query Execution Tracking
```bash
# Follow coordinator logs
docker logs -f axiom-spanner-coordinator | grep "spanning_query"

# Worker activity
docker logs -f backend-dimse-query-worker-1 | grep "dimse_query"
```

#### Error Investigation
```bash
# Recent errors
docker logs --since=1h axiom-spanner-coordinator | grep ERROR

# Database connection issues
docker logs dicom_processor_db | grep "connection"
```

### Support Commands

```bash
# Full system status
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps

# Restart enterprise services only
docker compose -f docker-compose.enterprise.yml restart

# Rebuild and restart
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build

# Clean restart (removes containers)
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml down
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build
```

## Next Steps

1. **Configure Your PACS Sources**: Add your 8 PACS systems to spanner configurations
2. **Scale Workers**: Increase worker count based on load testing
3. **Set Up Monitoring**: Configure alerts in your ELK stack
4. **Performance Testing**: Test with realistic query volumes
5. **Security Configuration**: Set up proper authentication and encryption

---

**Enterprise Support**: For assistance with scaling to 150+ locations, contact the development team with specific deployment requirements.
