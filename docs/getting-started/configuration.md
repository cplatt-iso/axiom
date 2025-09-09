# Configuration Guide

This guide covers all configuration options for Axiom, from basic environment setup to advanced enterprise configurations.

## Environment Configuration

### Core Environment Variables

#### Security Settings (Required)
```bash
# Cryptographic keys - MUST be changed in production
SECRET_KEY=your_secure_secret_key_here
JWT_SECRET_KEY=your_jwt_secret_key_here
POSTGRES_PASSWORD=secure_database_password

# Generate secure keys:
# python -c "import secrets; print(secrets.token_urlsafe(32))"
```

#### Instance Configuration
```bash
# Instance identification (used for multi-tenant deployments)
AXIOM_INSTANCE_ID=prod  # prod, staging, dev, etc.

# Environment type
NODE_ENV=production  # production, development, testing

# Debug mode (disable in production)
DEBUG=false
```

#### Database Configuration
```bash
# Primary database connection
DATABASE_URL=postgresql://axiom:${POSTGRES_PASSWORD}@postgres:5432/axiom

# Connection pool settings
DB_POOL_SIZE=20          # Number of permanent connections
DB_MAX_OVERFLOW=30       # Additional connections when pool is full
DB_POOL_TIMEOUT=30       # Seconds to wait for connection
DB_POOL_RECYCLE=3600     # Seconds before recreating connections

# Query optimization
DB_ECHO=false            # Log all SQL queries (debug only)
DB_STATEMENT_TIMEOUT=30  # Query timeout in seconds
```

#### Message Queue Configuration
```bash
# RabbitMQ connection
CELERY_BROKER_URL=amqp://axiom:${RABBITMQ_PASSWORD}@rabbitmq:5672//

# Task routing
CELERY_DEFAULT_QUEUE=celery
CELERY_HIGH_PRIORITY_QUEUE=high_priority
CELERY_LOW_PRIORITY_QUEUE=low_priority

# Worker settings
CELERY_WORKER_CONCURRENCY=4     # Processes per worker
CELERY_WORKER_PREFETCH=4        # Tasks to prefetch
CELERY_TASK_TIMEOUT=300         # Task timeout in seconds
CELERY_TASK_SOFT_TIMEOUT=270    # Soft timeout for graceful shutdown
```

#### Cache Configuration
```bash
# Redis connection
REDIS_URL=redis://redis:6379/0

# Cache settings
REDIS_MAX_CONNECTIONS=100       # Connection pool size
REDIS_SOCKET_TIMEOUT=5          # Socket timeout
REDIS_SOCKET_CONNECT_TIMEOUT=5  # Connection timeout
REDIS_RETRY_ON_TIMEOUT=true     # Retry on timeout

# Cache expiration (seconds)
CACHE_TTL_DEFAULT=3600          # 1 hour
CACHE_TTL_SESSION=86400         # 24 hours  
CACHE_TTL_CROSSWALK=7200        # 2 hours
```

### Security Configuration

#### Authentication
```bash
# Google OAuth configuration
GOOGLE_OAUTH_CLIENT_ID=your_google_oauth_client_id

# JWT token settings
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=60
JWT_REFRESH_TOKEN_EXPIRE_DAYS=30

# API key settings
API_KEY_LENGTH=32
API_KEY_PREFIX=ak_${AXIOM_INSTANCE_ID}
```

#### CORS and Security Headers
```bash
# Cross-Origin Resource Sharing
BACKEND_CORS_ORIGINS=https://axiom.yourdomain.com,https://admin.yourdomain.com

# Security settings
SECURE_COOKIES=true             # HTTPS-only cookies
CSRF_PROTECTION=true            # Enable CSRF protection
SESSION_SECURE=true             # Secure session cookies
SESSION_HTTPONLY=true           # HTTP-only session cookies
SESSION_SAMESITE=strict         # SameSite cookie policy
```

#### TLS Configuration
```bash
# TLS for DICOM communications
TLS_ENABLED=true
TLS_VERIFY_PEER=true
TLS_REQUIRE_PEER_CERT=true

# Certificate paths (for file-based certs)
TLS_CERT_PATH=/app/secrets/tls.crt
TLS_KEY_PATH=/app/secrets/tls.key
TLS_CA_PATH=/app/secrets/ca.crt

# Google Secret Manager (preferred for production)
TLS_CERT_SECRET_NAME=projects/your-project/secrets/axiom-tls-cert/versions/latest
TLS_KEY_SECRET_NAME=projects/your-project/secrets/axiom-tls-key/versions/latest
```

### Storage Configuration

#### File System Storage
```bash
# Local storage paths
DICOM_STORAGE_PATH=/data/dicom           # Processed DICOM files
TEMP_STORAGE_PATH=/data/temp             # Temporary processing files
BACKUP_STORAGE_PATH=/data/backups        # Local backups
LOG_STORAGE_PATH=/data/logs              # Application logs

# Storage policies
MAX_FILE_SIZE_MB=2048                    # Maximum DICOM file size
STORAGE_CLEANUP_DAYS=90                  # Days to keep temp files
COMPRESSION_ENABLED=true                 # Enable DICOM compression
```

#### Cloud Storage
```bash
# Google Cloud Storage
GCS_BUCKET_NAME=axiom-dicom-prod
GCS_PREFIX=studies/
GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-service-account.json

# AWS S3 (if using)
AWS_BUCKET_NAME=axiom-dicom-prod
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

### DICOM Configuration

#### Listener Settings
```bash
# Default DICOM listener
DICOM_AE_TITLE=AXIOM
DICOM_PORT=11112
DICOM_HOST=0.0.0.0

# Network settings
DICOM_MAX_PDU_SIZE=16384        # Maximum PDU size
DICOM_TIMEOUT=30                # Connection timeout
DICOM_MAX_ASSOCIATIONS=50       # Concurrent associations
```

#### Processing Settings
```bash
# Rule engine
RULE_ENGINE_ENABLED=true
LOG_ORIGINAL_ATTRIBUTES=true    # Log changes to Original Attributes Sequence
RULE_EXECUTION_TIMEOUT=60       # Rule execution timeout

# AI integration
AI_ENABLED=true
AI_PROVIDER=openai              # openai, vertex_ai
AI_MAX_RETRIES=3
AI_TIMEOUT=30

# OpenAI configuration
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4
OPENAI_MAX_TOKENS=1000

# Google Vertex AI configuration
VERTEX_AI_PROJECT_ID=your_project_id
VERTEX_AI_LOCATION=us-central1
VERTEX_AI_MODEL=gemini-pro
```

### Monitoring Configuration

#### Logging
```bash
# Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL=INFO

# Structured logging
STRUCTURED_LOGGING=true
LOG_FORMAT=json                 # json, text

# Log destinations
LOG_TO_CONSOLE=true
LOG_TO_FILE=true
LOG_FILE=/app/logs/axiom.log

# Log rotation
LOG_MAX_BYTES=100000000         # 100MB
LOG_BACKUP_COUNT=5              # Keep 5 old files

# Correlation tracking
CORRELATION_ID_HEADER=X-Correlation-ID
GENERATE_CORRELATION_ID=true
```

#### Metrics and Monitoring
```bash
# Prometheus metrics
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=9090
METRICS_PATH=/metrics

# Health check settings
HEALTH_CHECK_TIMEOUT=30
HEALTH_CHECK_INTERVAL=60

# Sentry error tracking
SENTRY_DSN=your_sentry_dsn_here
SENTRY_ENVIRONMENT=${AXIOM_INSTANCE_ID}
SENTRY_TRACES_SAMPLE_RATE=0.1
```

### Performance Configuration

#### API Performance
```bash
# FastAPI settings
WORKERS=4                       # Number of API workers
WORKER_CONNECTIONS=1000         # Connections per worker
KEEPALIVE=5                     # Keep-alive timeout

# Request limits
MAX_REQUEST_SIZE=2147483648     # 2GB for DICOM uploads
REQUEST_TIMEOUT=300             # Request timeout in seconds
```

#### Database Performance
```bash
# Query optimization
DB_SLOW_QUERY_THRESHOLD=1.0     # Log queries slower than 1 second
DB_EXPLAIN_ANALYZE=false        # Log query execution plans

# Connection management
DB_POOL_PRE_PING=true           # Validate connections before use
DB_POOL_RESET_ON_RETURN=commit  # Reset transaction state
```

#### Worker Performance
```bash
# Celery optimization
CELERY_OPTIMIZATION=fair        # Task distribution algorithm
CELERY_ACKS_LATE=true          # Acknowledge after completion
CELERY_REJECT_ON_WORKER_LOST=true

# Memory management
CELERY_MAX_MEMORY_PER_CHILD=400000  # KB, restart worker after limit
CELERY_MAX_TASKS_PER_CHILD=1000     # Restart worker after N tasks
```

## Configuration Files

### Docker Compose Configuration

#### Development Environment
```yaml
# docker-compose.dev.yml
version: '3.8'

services:
  api:
    build:
      context: .
      target: development
    environment:
      - DEBUG=true
      - LOG_LEVEL=DEBUG
    volumes:
      - ./app:/app/app:delegated
    ports:
      - "8001:8000"
    
  postgres:
    environment:
      - POSTGRES_DB=axiom_dev
    ports:
      - "5432:5432"
```

#### Production Environment
```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/ssl:/etc/nginx/ssl:ro
    
  api:
    deploy:
      replicas: 3
      resources:
        limits:
          memory: 4G
          cpus: '2'
        reservations:
          memory: 2G
          cpus: '1'
    
  worker:
    deploy:
      replicas: 4
      resources:
        limits:
          memory: 8G
          cpus: '4'
        reservations:
          memory: 4G
          cpus: '2'
```

### Nginx Configuration

#### Production Nginx Config
```nginx
# nginx/nginx.conf
upstream axiom_api {
    least_conn;
    server api_1:8000 max_fails=3 fail_timeout=30s;
    server api_2:8000 max_fails=3 fail_timeout=30s;
    server api_3:8000 max_fails=3 fail_timeout=30s;
}

server {
    listen 443 ssl http2;
    server_name axiom.yourdomain.com;

    # SSL configuration
    ssl_certificate /etc/nginx/ssl/axiom.crt;
    ssl_certificate_key /etc/nginx/ssl/axiom.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;

    # File upload limits for DICOM
    client_max_body_size 2G;
    client_body_timeout 300s;
    client_body_buffer_size 128k;

    # Proxy settings
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_http_version 1.1;

    location / {
        proxy_pass http://axiom_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }

    # Health check endpoint (no auth required)
    location /health {
        proxy_pass http://axiom_api/health;
        access_log off;
    }

    # Metrics endpoint (restrict access)
    location /metrics {
        allow 10.0.0.0/8;
        allow 172.16.0.0/12;
        allow 192.168.0.0/16;
        deny all;
        proxy_pass http://axiom_api/metrics;
    }
}
```

### Database Configuration

#### PostgreSQL Tuning
```sql
-- postgresql.conf optimizations
shared_buffers = 8GB                    # 25% of system RAM
effective_cache_size = 24GB             # 75% of system RAM
work_mem = 64MB                         # Per-operation memory
maintenance_work_mem = 2GB              # Maintenance operations
wal_buffers = 64MB                      # WAL buffer size
checkpoint_completion_target = 0.9      # Checkpoint timing
default_statistics_target = 100         # Query planner statistics
random_page_cost = 1.1                  # SSD optimization
effective_io_concurrency = 200          # Concurrent I/O operations

-- Performance monitoring
log_min_duration_statement = 1000       # Log slow queries (1s+)
log_checkpoints = on                    # Log checkpoint activity
log_connections = on                    # Log connections
log_disconnections = on                 # Log disconnections
log_lock_waits = on                     # Log lock waits
```

### Environment-Specific Configurations

#### Development
```bash
# .env.dev
DEBUG=true
LOG_LEVEL=DEBUG
SECURE_COOKIES=false
CORS_ALLOW_ALL=true
DB_ECHO=true
CELERY_TASK_ALWAYS_EAGER=true  # Execute tasks synchronously
AI_ENABLED=false               # Disable AI in dev
PROMETHEUS_ENABLED=false       # Disable metrics in dev
```

#### Staging
```bash
# .env.staging
DEBUG=false
LOG_LEVEL=INFO
SECURE_COOKIES=true
DB_ECHO=false
CELERY_TASK_ALWAYS_EAGER=false
AI_ENABLED=true
PROMETHEUS_ENABLED=true
SENTRY_ENVIRONMENT=staging
```

#### Production
```bash
# .env.prod
DEBUG=false
LOG_LEVEL=WARNING
SECURE_COOKIES=true
CSRF_PROTECTION=true
SESSION_SECURE=true
DB_ECHO=false
PROMETHEUS_ENABLED=true
SENTRY_ENVIRONMENT=production
HEALTH_CHECK_ENABLED=true
```

## Advanced Configuration

### Multi-Instance Deployment

For high availability, you can run multiple Axiom instances:

```bash
# Instance 1
AXIOM_INSTANCE_ID=prod_east
DICOM_PORT=11112
REDIS_URL=redis://redis-east:6379/0

# Instance 2  
AXIOM_INSTANCE_ID=prod_west
DICOM_PORT=11113
REDIS_URL=redis://redis-west:6379/0
```

### Load Balancing Configuration

```yaml
# docker-compose.lb.yml
version: '3.8'

services:
  haproxy:
    image: haproxy:2.8
    ports:
      - "80:80"
      - "443:443"
      - "11112:11112"  # DICOM load balancing
    volumes:
      - ./haproxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    depends_on:
      - api_east
      - api_west
```

### Monitoring Stack

```yaml
# docker-compose.monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources:ro

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
```

## Configuration Validation

### Validation Script
```bash
#!/bin/bash
# validate-config.sh

echo "Validating Axiom configuration..."

# Check required environment variables
REQUIRED_VARS="SECRET_KEY POSTGRES_PASSWORD GOOGLE_OAUTH_CLIENT_ID"
for var in $REQUIRED_VARS; do
    if [ -z "${!var}" ]; then
        echo "ERROR: Required variable $var is not set"
        exit 1
    fi
done

# Validate database connection
if ! docker-compose exec -T postgres psql -U axiom -d axiom -c "SELECT 1;" > /dev/null 2>&1; then
    echo "ERROR: Database connection failed"
    exit 1
fi

# Check API health
if ! curl -f http://localhost:8001/health > /dev/null 2>&1; then
    echo "ERROR: API health check failed"
    exit 1
fi

echo "Configuration validation passed!"
```

### Configuration Best Practices

1. **Security**:
   - Use strong, unique passwords and keys
   - Enable TLS for all communications
   - Restrict network access with firewalls
   - Regular security audits and updates

2. **Performance**:
   - Tune database connection pools
   - Configure appropriate worker counts
   - Monitor resource usage and scale accordingly
   - Use caching for frequently accessed data

3. **Reliability**:
   - Configure proper backup strategies
   - Set up monitoring and alerting
   - Use health checks for all services
   - Plan for disaster recovery

4. **Compliance**:
   - Enable audit logging
   - Configure data retention policies
   - Implement access controls
   - Document configuration changes

---

This configuration guide provides comprehensive coverage of all Axiom settings. Adjust values based on your specific environment, scale, and requirements.
