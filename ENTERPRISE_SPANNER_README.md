# Enterprise DICOM Query Spanning System

> 🏥 Production-ready DICOM query spanning across multiple PACS systems using microservices architecture.

## Overview

The Enterprise DICOM Query Spanning System is a horizontally scalable microservices architecture designed to handle massive scale medical imaging workflows. Built for enterprises operating **150+ locations**, **1200+ modalities**, and **8+ PACS systems**.

### Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   API Gateway   │    │   Frontend UI   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          v                      v                      v
┌─────────────────────────────────────────────────────────────────┐
│                    Main API (FastAPI)                          │
└─────────────────┬───────────────────────────────────┬─────────┘
                  │                                   │
                  v                                   v
┌─────────────────────────────────────┐    ┌─────────────────────┐
│        Spanner Coordinator          │    │    DIMSE SCP       │
│     (Orchestration Service)         │    │    Listener         │
└─────────┬─────────────────┬─────────┘    └─────────────────────┘
          │                 │
          v                 v
┌─────────────────┐    ┌─────────────────┐
│    RabbitMQ     │    │      Redis      │
│  (Task Queue)   │    │    (Cache)      │
└─────────┬───────┘    └─────────────────┘
          │
          v
┌─────────────────────────────────────────────────────────────────┐
│                    DIMSE Query Workers                          │
│              (12-24 instances for production)                   │
└─────────────────────────────────────────────────────────────────┘
          │
          v
┌─────────────────────────────────────────────────────────────────┐
│                    PACS Systems (8 systems)                    │
│         PACS-A    PACS-B    PACS-C    ...    PACS-H           │
└─────────────────────────────────────────────────────────────────┘
```

## Production Scale Specifications

- **Locations**: 150+ medical facilities
- **Modalities**: 1200+ imaging devices (CT, MRI, X-Ray, etc.)
- **PACS Systems**: 8 different vendor systems
- **Concurrent Queries**: 1000+ simultaneous spanning queries
- **Data Volume**: Multi-petabyte DICOM archives
- **Response Time**: <500ms for metadata queries
- **Availability**: 99.9% uptime SLA

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- PostgreSQL 14+
- 16GB+ RAM recommended for production

### 1. Clone and Setup

```bash
git clone <your-repo>
cd axiom/backend
```

### 2. Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit configuration for your environment
vim .env
```

### 3. Start Enterprise Services

```bash
# Start the entire enterprise stack
./scripts/start_enterprise_spanner.sh
```

### 4. Monitor System Health

```bash
# Real-time monitoring
./scripts/monitor_spanner.sh
```

## Configuration

### Spanner Configuration

Create spanning configurations for your enterprise setup:

```bash
curl -X POST 'http://localhost:8001/api/v1/config/spanner/' \
  -H 'Authorization: Api-Key YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "Enterprise Production",
    "description": "Production spanning across all 8 PACS",
    "max_concurrent_sources": 8,
    "timeout_seconds": 30,
    "retry_attempts": 2,
    "failure_strategy": "partial_results",
    "deduplication_enabled": true,
    "enterprise_mode": true
  }'
```

### Source Mapping

Map your PACS systems to the spanner:

```bash
curl -X POST 'http://localhost:8001/api/v1/config/spanner/1/sources/' \
  -H 'Authorization: Api-Key YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "source_type": "modality",
    "source_id": 1,
    "priority": 1,
    "timeout_override": 20
  }'
```

## Production Scaling

### Horizontal Scaling Commands

```bash
# Scale DIMSE workers (recommended: 2-3 per PACS)
docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=24

# Scale coordinators for high availability
docker-compose -f docker-compose.enterprise.yml up -d --scale spanner-coordinator=5

# Scale SCP listeners for load balancing
docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-scp-listener=3
```

### Production Deployment Recommendations

| Component | Instances | CPU | Memory | Notes |
|-----------|-----------|-----|--------|-------|
| API Gateway | 3 | 2 cores | 4GB | Load balanced |
| Spanner Coordinator | 5 | 4 cores | 8GB | Redis cluster |
| DIMSE Workers | 24 | 2 cores | 4GB | 3 per PACS |
| SCP Listeners | 3 | 2 cores | 4GB | High availability |
| PostgreSQL | 3 | 8 cores | 16GB | Primary + replicas |
| Redis | 6 | 4 cores | 8GB | Cluster mode |
| RabbitMQ | 3 | 4 cores | 8GB | Clustered |

## API Usage

### Query Spanning

Execute spanning queries across all configured PACS:

```bash
# Studies spanning
curl -X GET 'http://localhost:8001/api/v1/spanner/qido/studies' \
  -H 'Authorization: Api-Key YOUR_API_KEY' \
  -G \
  -d 'PatientID=12345' \
  -d 'spanner_config_id=1' \
  -d 'async=true'

# Series spanning  
curl -X GET 'http://localhost:8001/api/v1/spanner/qido/series' \
  -H 'Authorization: Api-Key YOUR_API_KEY' \
  -G \
  -d 'StudyInstanceUID=1.2.3.4.5' \
  -d 'spanner_config_id=1'

# Instances spanning
curl -X GET 'http://localhost:8001/api/v1/spanner/qido/instances' \
  -H 'Authorization: Api-Key YOUR_API_KEY' \
  -G \
  -d 'SeriesInstanceUID=1.2.3.4.5.6' \
  -d 'spanner_config_id=1'
```

### Real-time Status

Monitor spanning operations in real-time:

```bash
# Query status
curl -X GET 'http://localhost:8001/api/v1/spanner/queries/abc123/status' \
  -H 'Authorization: Api-Key YOUR_API_KEY'

# Live metrics
curl -X GET 'http://localhost:8002/metrics' \
  -H 'Authorization: Api-Key YOUR_API_KEY'
```

## Monitoring & Observability

### Built-in Dashboards

- **Prometheus**: http://localhost:9090 - Metrics collection
- **Grafana**: http://localhost:3000 - Visualization dashboards
- **RabbitMQ Management**: http://localhost:15672 - Queue monitoring

### Key Metrics

- Query throughput (queries/second)
- Response time percentiles
- Worker utilization
- Queue depth
- Cache hit rates
- PACS connection health
- Error rates by source

### Alerting

Configure alerts for:
- High query failure rates (>5%)
- Worker saturation (>80% CPU)
- Queue backlog (>1000 pending)
- PACS connection failures
- Response time degradation (>2s)

## Security

### Authentication

All endpoints require API key authentication:

```bash
curl -H 'Authorization: Api-Key YOUR_SECURE_API_KEY' \
  'http://localhost:8001/api/v1/spanner/...'
```

### Network Security

- TLS 1.3 for all external communications
- VPN or private networks for PACS connections
- Firewall rules restricting DICOM ports
- Certificate-based authentication for DIMSE

### Data Protection

- DICOM data encryption at rest
- PHI tokenization for logs
- Audit trails for all queries
- HIPAA compliance measures

## Troubleshooting

### Common Issues

1. **High Memory Usage**
   ```bash
   # Check container memory
   docker stats
   
   # Scale down workers if needed
   docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=8
   ```

2. **Queue Backlog**
   ```bash
   # Check queue status
   docker-compose -f docker-compose.enterprise.yml exec rabbitmq rabbitmqctl list_queues
   
   # Scale up workers
   docker-compose -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=32
   ```

3. **PACS Connection Issues**
   ```bash
   # Test PACS connectivity
   curl -X GET 'http://localhost:8001/api/v1/sources/1/health' \
     -H 'Authorization: Api-Key YOUR_API_KEY'
   ```

### Log Analysis

```bash
# View coordinator logs
docker-compose -f docker-compose.enterprise.yml logs -f spanner-coordinator

# View worker logs
docker-compose -f docker-compose.enterprise.yml logs -f dimse-query-worker

# View specific container logs
docker logs axiom-enterprise-spanner_dimse-query-worker_1
```

## Performance Tuning

### Database Optimization

```sql
-- Index optimization for query logs
CREATE INDEX CONCURRENTLY idx_spanner_query_log_created_status 
ON spanner_query_log(created_at, status);

-- Partition large query log tables
CREATE TABLE spanner_query_log_2024 PARTITION OF spanner_query_log
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Redis Configuration

```redis
# Memory optimization
maxmemory 8gb
maxmemory-policy allkeys-lru

# Persistence for critical data
save 900 1
save 300 10
save 60 10000
```

### RabbitMQ Tuning

```bash
# Increase queue limits
rabbitmqctl set_policy spanner-policy "spanner.*" '{"max-length":10000,"overflow":"reject-publish"}'

# Configure clustering
rabbitmqctl join_cluster rabbit@node1
```

## Development

### Adding New Features

1. Create feature branch
2. Add tests in `app/tests/`
3. Update API documentation
4. Test with enterprise scaling
5. Submit pull request

### Local Development

```bash
# Start minimal stack for development
docker-compose -f docker-compose.yml up -d

# Run tests
pytest app/tests/

# Code quality checks
black app/
flake8 app/
mypy app/
```

## Support

### Enterprise Support

- 24/7 technical support
- Dedicated account manager
- Priority bug fixes
- Custom feature development
- On-site deployment assistance

### Community

- GitHub Issues: Report bugs and request features
- Documentation: Comprehensive API docs
- Slack Channel: Real-time community support

## License

Enterprise license with commercial support available.

---

**🎯 Built for scale. Ready for production. Spanning DICOM queries across 150 locations and 8 PACS systems.**
