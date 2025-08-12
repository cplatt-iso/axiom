# Docker Compose Architecture - Enterprise DICOM Spanning

## Overview

The enterprise DICOM spanning system uses a sophisticated Docker Compose overlay architecture that extends your existing DICOM infrastructure without disruption.

## Architecture Components

### File Structure
```
axiom/backend/
├── docker-compose.yml              # Original DICOM system (PRESERVED)
├── docker-compose.enterprise.yml   # Enterprise overlay (NEW)
├── Dockerfile.spanner-coordinator   # Coordinator service
├── Dockerfile.dimse-worker         # Query workers
├── Dockerfile.dimse-scp            # SCP listener
└── services/enterprise/            # Enterprise microservices
```

### Deployment Strategy

```bash
# Original system (unchanged)
docker compose up -d

# Enterprise extension (overlay)
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build
```

## Service Breakdown

### Original System Services (docker-compose.yml)

#### Core Infrastructure
```yaml
services:
  db:                    # PostgreSQL database
    image: postgres:15-alpine
    ports: ["5432:5432"]
    healthcheck: pg_isready
    
  redis:                 # Cache and session store
    image: redis:7-alpine
    healthcheck: redis-cli ping
    
  rabbitmq:              # Message queue
    image: rabbitmq with management
    ports: ["5672:5672", "15672:15672"]
    healthcheck: rabbitmqctl status
```

#### DICOM Processing Services
```yaml
  api:                   # Main FastAPI application
    build: Dockerfile
    ports: ["8001:80"]
    depends_on: [db, redis, rabbitmq]
    
  worker:                # Celery worker for DICOM processing
    build: Dockerfile.worker
    depends_on: [rabbitmq, redis, db]
    
  beat:                  # Celery beat scheduler
    build: Dockerfile.worker
    command: celery beat
    depends_on: [rabbitmq, redis, db]
```

#### DICOM Listeners
```yaml
  listener:              # DIMSE SCP listener (port 11112)
    build: Dockerfile.listener
    ports: ["11112:11112"]
    
  listener_2:            # Additional DIMSE SCP (port 11113)
    build: Dockerfile.listener
    ports: ["11113:11113"]
    
  mllp-listener:         # HL7 MLLP listener (port 2575)
    build: Dockerfile
    ports: ["2575:2575"]
```

### Enterprise Extension Services (docker-compose.enterprise.yml)

#### Spanning Coordinator
```yaml
spanner-coordinator:
  build:
    context: .
    dockerfile: Dockerfile.spanner-coordinator
  container_name: axiom-spanner-coordinator
  ports: ["8002:8000"]
  environment:
    - DATABASE_URL=postgresql://...
    - REDIS_URL=redis://redis:6379/0
    - RABBITMQ_URL=amqp://...
  depends_on:
    db: { condition: service_healthy }
    redis: { condition: service_healthy }
    rabbitmq: { condition: service_healthy }
  networks: [default, shared]
  logging:
    driver: "fluentd"
    options:
      fluentd-address: "127.0.0.1:24224"
      tag: "axiom.spanner_coordinator.{{.Name}}"
```

#### DIMSE Query Workers
```yaml
dimse-query-worker:
  build:
    context: .
    dockerfile: Dockerfile.dimse-worker
  environment:
    - WORKER_TYPE=dimse-query
    - DATABASE_URL=...
    - REDIS_URL=...
    - RABBITMQ_URL=...
  depends_on:
    db: { condition: service_healthy }
    redis: { condition: service_healthy }
    rabbitmq: { condition: service_healthy }
  # Scalable: docker compose up --scale dimse-query-worker=5
```

#### Enterprise DIMSE SCP Listener
```yaml
dimse-scp-listener:
  build:
    context: .
    dockerfile: Dockerfile.dimse-scp
  ports: ["11114:11114"]  # Additional DICOM endpoint
  environment:
    - AXIOM_INSTANCE_ID=spanner_scp
    - DATABASE_URL=...
  depends_on:
    db: { condition: service_healthy }
    redis: { condition: service_healthy }
    rabbitmq: { condition: service_healthy }
```

## Network Architecture

### Network Topology
```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Host Network                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐                      │
│  │  Original API   │    │ Spanner Coord.  │                      │
│  │   Port 8001     │    │   Port 8002     │                      │
│  └─────────────────┘    └─────────────────┘                      │
│           │                       │                             │
│           └───────────┬───────────┘                             │
│                       │                                         │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                 Internal Networks                          │ │
│  │                                                             │ │
│  │  default:     db, redis, rabbitmq, api, workers            │ │
│  │  shared:      cross-service communication                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  External Interfaces                       │ │
│  │                                                             │ │
│  │  DIMSE Listeners: 11112, 11113, 11114                      │ │
│  │  MLLP Listener:   2575                                     │ │
│  │  Database:        5432                                     │ │
│  │  RabbitMQ Mgmt:   15672                                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Port Mapping Strategy

| Service | Internal Port | External Port | Purpose |
|---------|---------------|---------------|---------|
| **Original API** | 80 | 8001 | Main DICOM API & Spanner management |
| **Spanner Coordinator** | 8000 | 8002 | Enterprise spanning orchestration |
| **DIMSE SCP (Original)** | 11112 | 11112 | Primary DICOM listener |
| **DIMSE SCP (Secondary)** | 11113 | 11113 | Secondary DICOM listener |
| **DIMSE SCP (Enterprise)** | 11114 | 11114 | Enterprise spanning listener |
| **MLLP Listener** | 2575 | 2575 | HL7 message processing |
| **PostgreSQL** | 5432 | 5432 | Database access |
| **RabbitMQ Management** | 15672 | 15672 | Queue management UI |

## Service Dependencies

### Dependency Graph
```
Enterprise Services:
  spanner-coordinator
    ├── depends_on: db (healthy)
    ├── depends_on: redis (healthy)
    └── depends_on: rabbitmq (healthy)
    
  dimse-query-worker
    ├── depends_on: db (healthy)
    ├── depends_on: redis (healthy)
    └── depends_on: rabbitmq (healthy)
    
  dimse-scp-listener
    ├── depends_on: db (healthy)
    ├── depends_on: redis (healthy)
    └── depends_on: rabbitmq (healthy)

Original Services:
  api, worker, beat, listeners
    ├── depends_on: db (healthy)
    ├── depends_on: redis (healthy)
    └── depends_on: rabbitmq (healthy)
```

### Health Check Strategy
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
  interval: 30s
  timeout: 10s
  start_period: 30s
  retries: 3
```

## Volume and Data Management

### Shared Volumes
```yaml
volumes:
  # Shared application logs
  - ./logs:/app/logs
  
  # GCP service account key
  - ./axiom-flow-gcs-key.json:/etc/gcp/axiom-flow-gcs-key.json:ro
  
  # Database persistence (from original compose)
  postgres_data:
    external: true
    
  rabbitmq_data:
    external: true
```

### Data Flow
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client App    │───▶│ Spanner Coord.  │───▶│ Query Workers   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   PostgreSQL    │    │   RabbitMQ      │
                       │  (Persistence)  │    │ (Job Queue)     │
                       └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │     Redis       │    │  PACS Systems   │
                       │   (Caching)     │    │ (External DIMSE)│
                       └─────────────────┘    └─────────────────┘
```

## Logging Architecture

### Fluentd Integration
```yaml
logging:
  driver: "fluentd"
  options:
    fluentd-address: "127.0.0.1:24224"
    tag: "axiom.{service_name}.{{.Name}}"
    mode: non-blocking
    max-buffer-size: "10m"
    fluentd-retry-wait: "1s"
    fluentd-max-retries: "60"
```

### Log Flow
```
Docker Containers → Fluentd (24224) → Elasticsearch → Kibana Dashboard
```

### Log Tags
- `axiom.spanner_coordinator.*`
- `axiom.dimse_query_worker.*`
- `axiom.dimse_scp_listener.*`
- `axiom.api.*`
- `axiom.worker.*`

## Scaling Configuration

### Horizontal Scaling
```bash
# Scale query workers
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=10

# Scale coordinators (load balancing)
docker compose -f docker-compose.enterprise.yml up -d --scale spanner-coordinator=3
```

### Resource Limits
```yaml
deploy:
  resources:
    limits:
      memory: 1G
      cpus: '0.5'
    reservations:
      memory: 512M
      cpus: '0.25'
```

## Configuration Management

### Environment Variables
```bash
# Database connection
DATABASE_URL=postgresql://user:pass@db:5432/dbname

# Cache configuration
REDIS_URL=redis://redis:6379/0

# Message queue
RABBITMQ_URL=amqp://user:pass@rabbitmq:5672/

# Application settings
PYTHONPATH=/app
WORKER_TYPE=dimse-query
AXIOM_INSTANCE_ID=spanner_scp

# External services
GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp/key.json
```

### Runtime Configuration
```yaml
env_file:
  - .env                 # Environment-specific variables
  - .env.local          # Local overrides (optional)
```

## Security Considerations

### Network Security
- Internal services communicate via Docker networks
- Only necessary ports exposed to host
- External PACS connections through firewall rules

### Authentication
- Service-to-service communication via internal networks
- External API authentication required
- Database credentials via environment variables

### Data Protection
- Sensitive configuration in environment variables
- Service account keys mounted read-only
- Database connections encrypted

## Maintenance Operations

### Start/Stop Services
```bash
# Start all services
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d

# Stop enterprise services only
docker compose -f docker-compose.enterprise.yml down

# Stop everything
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml down
```

### Updates and Rebuilds
```bash
# Rebuild enterprise services
docker compose -f docker-compose.enterprise.yml build

# Rolling update
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build

# Force recreation
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --force-recreate
```

### Backup Considerations
- Database: Standard PostgreSQL backup procedures
- Configuration: Version control docker-compose files
- Logs: ELK stack handles log retention
- Volumes: Include shared volumes in backup strategy

This architecture provides a robust, scalable foundation for enterprise DICOM query spanning while maintaining compatibility with your existing infrastructure.
