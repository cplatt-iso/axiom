# DICOM Query Spanning Implementation

This document describes the complete DICOM Query Spanning system implementation for the Axiom backend.

## Overview

The DICOM Query Spanning system enables distributed queries across multiple PACS/DICOM sources with intelligent result aggregation, deduplication, and failure handling. It supports both DIMSE (C-FIND, C-MOVE, C-GET) and DICOMweb (QIDO-RS, WADO-RS) protocols.

## Architecture

### Core Components

1. **SpannerEngine** (`app/services/spanner_engine.py`)
   - Core query execution engine
   - Parallel query processing across multiple sources
   - Result aggregation and deduplication
   - Configurable failure strategies

2. **Database Models** (`app/db/models/spanner.py`)
   - `SpannerConfig`: Main configuration for spanning behavior
   - `SpannerSourceMapping`: Maps sources to spanner configs with priorities
   - `SpannerQueryLog`: Audit trail for all spanning operations

3. **DIMSE Services**
   - `SpannerSCP` (`app/services/dimse/spanner_scp.py`): Accepts C-FIND queries
   - `CMoveProxyService` (`app/services/dimse/cmove_proxy.py`): Handles C-MOVE operations
   - `SpannerServiceManager` (`app/services/dimse/spanner_service_manager.py`): Service orchestration

4. **DICOMweb Endpoints**
   - `spanner_qido.py`: QIDO-RS spanning endpoints
   - `spanner_wado.py`: WADO-RS spanning endpoints

5. **Configuration API** (`app/api/api_v1/endpoints/config_spanner.py`)
   - REST API for managing spanner configurations
   - Source mapping management
   - Testing and validation endpoints

## Configuration

### Spanner Configuration Fields

```python
{
    "name": "Production Spanning Config",
    "description": "Primary spanning configuration for production queries",
    "max_concurrent_sources": 5,
    "query_timeout_seconds": 30,
    "failure_strategy": "BEST_EFFORT",  # FAIL_FAST, BEST_EFFORT, MINIMUM_THRESHOLD
    "minimum_success_threshold": 2,
    "deduplication_strategy": "MOST_COMPLETE",  # FIRST_WINS, MOST_COMPLETE, MERGE_ALL
    "supports_dimse_scp": true,
    "supports_qido": true,
    "supports_wado": true,
    "supports_cmove": true,
    "is_enabled": true
}
```

### Source Mapping Fields

```python
{
    "spanner_config_id": 1,
    "source_id": 5,  # References existing DIMSE Q/R source
    "priority": 1,   # Lower number = higher priority
    "timeout_seconds": 30,
    "max_retries": 2,
    "is_enabled": true
}
```

## API Endpoints

### Configuration Management

- `GET /api/v1/config/spanner/` - List all spanner configs
- `POST /api/v1/config/spanner/` - Create new spanner config
- `GET /api/v1/config/spanner/{id}` - Get specific config
- `PUT /api/v1/config/spanner/{id}` - Update config
- `DELETE /api/v1/config/spanner/{id}` - Delete config

### Source Mapping Management

- `POST /api/v1/config/spanner/{id}/sources` - Add source to config
- `GET /api/v1/config/spanner/{id}/sources` - List sources for config
- `DELETE /api/v1/config/spanner/{id}/sources/{source_id}` - Remove source

### QIDO-RS Spanning

- `GET /api/v1/spanner/qido/studies` - Search studies across sources
- `GET /api/v1/spanner/qido/studies/{study_uid}/series` - Search series
- `GET /api/v1/spanner/qido/studies/{study_uid}/series/{series_uid}/instances` - Search instances

### WADO-RS Spanning

- `GET /api/v1/spanner/wado/studies/{study_uid}` - Retrieve study from best source
- `GET /api/v1/spanner/wado/studies/{study_uid}/series/{series_uid}` - Retrieve series
- `GET /api/v1/spanner/wado/studies/{study_uid}/metadata` - Get consolidated metadata

### Service Management

- `GET /api/v1/spanner/services/status` - Get all service status
- `POST /api/v1/spanner/services/start` - Start all spanner services
- `POST /api/v1/spanner/services/stop` - Stop all services
- `POST /api/v1/spanner/services/restart/{service_id}` - Restart specific service
- `POST /api/v1/spanner/services/cmove` - Execute C-MOVE request

## Strategies

### Failure Strategies

1. **FAIL_FAST**: Stop on first source failure
2. **BEST_EFFORT**: Continue despite failures, return partial results
3. **MINIMUM_THRESHOLD**: Require minimum number of successful sources

### Deduplication Strategies

1. **FIRST_WINS**: Use first result encountered for duplicates
2. **MOST_COMPLETE**: Use result with most populated fields
3. **MERGE_ALL**: Combine data from all sources

### C-MOVE Strategies

1. **DIRECT**: Provide client with source connection details
2. **PROXY**: Route all data through spanner service
3. **HYBRID**: Try direct first, fallback to proxy

## Deployment

### Standalone Service

```bash
# Start as systemd service
sudo systemctl enable axiom-spanner
sudo systemctl start axiom-spanner

# Check status
sudo systemctl status axiom-spanner
```

### Docker Deployment

```bash
# Build and start spanner service
docker-compose -f docker-compose.spanner.yml up -d

# Check logs
docker-compose -f docker-compose.spanner.yml logs -f axiom-spanner
```

### Manual Start

```bash
# From backend directory
python scripts/start_spanner_services.py
```

## Database Setup

The spanner system requires database tables created via Alembic migration:

```bash
# Run migration
alembic upgrade head
```

Migration file: `alembic/versions/20a5e96f8a5a_add_spanner_configuration_tables.py`

## Monitoring

### Service Health

```bash
# Check service status via API
curl -X GET "http://localhost:8000/api/v1/spanner/services/status" \
  -H "Authorization: Bearer <token>"
```

### Query Logs

```bash
# Get recent spanning operations
curl -X GET "http://localhost:8000/api/v1/spanner/services/logs?limit=50" \
  -H "Authorization: Bearer <token>"
```

### Metrics

The system logs the following metrics:
- Query execution time
- Source response times
- Success/failure rates
- Deduplication statistics
- Resource utilization

## Example Usage

### 1. Create Spanner Configuration

```bash
curl -X POST "http://localhost:8000/api/v1/config/spanner/" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "name": "Multi-Site Spanning",
    "description": "Spans across main and backup PACS",
    "max_concurrent_sources": 3,
    "query_timeout_seconds": 30,
    "failure_strategy": "BEST_EFFORT",
    "deduplication_strategy": "MOST_COMPLETE",
    "supports_dimse_scp": true,
    "supports_qido": true,
    "supports_wado": true,
    "supports_cmove": true
  }'
```

### 2. Add Sources to Configuration

```bash
# Add primary PACS
curl -X POST "http://localhost:8000/api/v1/config/spanner/1/sources" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "source_id": 1,
    "priority": 1,
    "timeout_seconds": 20
  }'

# Add backup PACS
curl -X POST "http://localhost:8000/api/v1/config/spanner/1/sources" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "source_id": 2,
    "priority": 2,
    "timeout_seconds": 30
  }'
```

### 3. Perform Spanning Query

```bash
# QIDO-RS spanning search
curl -X GET "http://localhost:8000/api/v1/spanner/qido/studies?PatientID=12345&spanner_config_id=1" \
  -H "Authorization: Bearer <token>"
```

### 4. Execute C-MOVE

```bash
curl -X POST "http://localhost:8000/api/v1/spanner/services/cmove" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "study_uid": "1.2.3.4.5.6.7.8.9",
    "destination_ae": "WORKSTATION",
    "spanner_config_id": 1,
    "move_strategy": "HYBRID"
  }'
```

## Security Considerations

1. **Authentication**: All API endpoints require valid user authentication
2. **IP Validation**: Source mappings can include IP restrictions
3. **Resource Limits**: Configurable timeouts and concurrency limits
4. **Audit Trail**: All operations logged to database
5. **TLS Support**: DIMSE connections support TLS where configured

## Performance Tuning

### Concurrent Sources
- Balance between speed and resource usage
- Recommended: 3-5 concurrent sources max

### Timeouts
- Set source timeouts based on network latency
- Query timeout should be > max source timeout

### Deduplication
- FIRST_WINS: Fastest, least accurate
- MOST_COMPLETE: Good balance of speed and accuracy
- MERGE_ALL: Slowest, most comprehensive

## Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   # Check service status
   curl http://localhost:8000/api/v1/spanner/services/status
   
   # Check logs
   journalctl -u axiom-spanner -f
   ```

2. **Database connection issues**
   ```bash
   # Test database connection
   python -c "from app.db.session import SessionLocal; SessionLocal().execute('SELECT 1')"
   ```

3. **DIMSE connection failures**
   ```bash
   # Check DIMSE listener status
   curl http://localhost:8000/api/v1/config/dimse-listeners/
   ```

### Performance Issues

1. **Slow queries**: Check source timeouts and network connectivity
2. **High memory usage**: Reduce max_concurrent_sources
3. **Database locks**: Check for long-running transactions

## Future Enhancements

1. **Load Balancing**: Round-robin source selection
2. **Caching**: Query result caching with TTL
3. **Metrics Dashboard**: Real-time performance monitoring
4. **Auto-failover**: Automatic source health detection
5. **Query Optimization**: Smart query routing based on content

## API Testing

A comprehensive test suite is included in the implementation:

```bash
# Test configuration API
python -m pytest app/tests/test_spanner_config.py

# Test spanning engine
python -m pytest app/tests/test_spanner_engine.py

# Test DIMSE services
python -m pytest app/tests/test_spanner_dimse.py
```

See the individual test files for detailed API usage examples.
