# Troubleshooting Guide

This guide helps diagnose and resolve common issues with Axiom deployments. Issues are organized by category with step-by-step resolution procedures.

## Quick Diagnostics

### Health Check Script
First, run this comprehensive health check:

```bash
#!/bin/bash
# health-check.sh

echo "=== Axiom Health Check ==="
echo "Date: $(date)"
echo "Host: $(hostname)"
echo ""

# Check Docker
echo "1. Docker Status:"
if docker --version > /dev/null 2>&1; then
    echo "✓ Docker is installed: $(docker --version)"
    if docker ps > /dev/null 2>&1; then
        echo "✓ Docker daemon is running"
    else
        echo "✗ Docker daemon is not running"
        exit 1
    fi
else
    echo "✗ Docker is not installed"
    exit 1
fi

# Check containers
echo ""
echo "2. Container Status:"
./axiomctl ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Check API health
echo ""
echo "3. API Health:"
if curl -f -s http://localhost:8001/health > /dev/null; then
    API_HEALTH=$(curl -s http://localhost:8001/health | jq -r '.status' 2>/dev/null || echo "unknown")
    echo "✓ API is responding: $API_HEALTH"
else
    echo "✗ API is not responding"
fi

# Check database
echo ""
echo "4. Database Connection:"
if ./axiomctl exec -T postgres psql -U axiom -d axiom -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✓ Database is accessible"
else
    echo "✗ Database connection failed"
fi

# Check workers
echo ""
echo "5. Worker Status:"
ACTIVE_WORKERS=$(./axiomctl exec -T api python -c "
from app.core.celery_app import celery_app
try:
    inspect = celery_app.control.inspect()
    active = inspect.active()
    print(len(active) if active else 0)
except Exception as e:
    print('error')
" 2>/dev/null)

if [ "$ACTIVE_WORKERS" = "error" ]; then
    echo "✗ Cannot check worker status"
elif [ "$ACTIVE_WORKERS" -gt 0 ]; then
    echo "✓ Workers are active: $ACTIVE_WORKERS"
else
    echo "✗ No active workers found"
fi

# Check disk space
echo ""
echo "6. Disk Space:"
df -h | grep -E "(/$|/data|/var/lib/docker)" | while read line; do
    usage=$(echo $line | awk '{print $5}' | tr -d '%')
    if [ "$usage" -gt 90 ]; then
        echo "⚠ High disk usage: $line"
    else
        echo "✓ Disk usage OK: $line"
    fi
done

echo ""
echo "=== Health Check Complete ==="
```

### Log Collection Script
Collect logs for debugging:

```bash
#!/bin/bash
# collect-logs.sh

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="axiom_logs_$TIMESTAMP"

mkdir -p $LOG_DIR

echo "Collecting Axiom logs..."

# Container logs
for service in api worker beat postgres redis rabbitmq; do
    echo "Collecting $service logs..."
    ./axiomctl logs --tail=1000 $service > $LOG_DIR/${service}.log 2>&1
done

# System logs
echo "Collecting system logs..."
journalctl -u docker.service --since "1 hour ago" > $LOG_DIR/docker_system.log 2>&1

# Configuration
echo "Collecting configuration..."
cp .env $LOG_DIR/env_config.txt 2>/dev/null || echo "No .env file found" > $LOG_DIR/env_config.txt
./axiomctl config > $LOG_DIR/docker_config.txt 2>&1

# System info
echo "Collecting system info..."
cat > $LOG_DIR/system_info.txt << EOF
Hostname: $(hostname)
Date: $(date)
Uptime: $(uptime)
Memory: $(free -h)
Disk: $(df -h)
Docker version: $(docker --version)
Docker compose version: $(docker-compose --version)
Kernel: $(uname -a)
EOF

# Create archive
tar -czf ${LOG_DIR}.tar.gz $LOG_DIR
rm -rf $LOG_DIR

echo "Logs collected in: ${LOG_DIR}.tar.gz"
```

## Container and Service Issues

### Services Won't Start

#### Symptom: `docker-compose up` fails
**Diagnosis:**
```bash
# Check for specific error messages
./axiomctl up

# Check individual service logs
./axiomctl logs postgres
./axiomctl logs redis
./axiomctl logs rabbitmq
```

**Common Causes & Solutions:**

1. **Port Conflicts**
   ```bash
   # Check what's using the ports
   sudo netstat -tulpn | grep -E ':(8001|5432|6379|5672|15672)'
   sudo lsof -i :8001
   
   # Solution: Change ports in docker-compose.yml
   ports:
     - "8002:8000"  # Use different host port
   ```

2. **Insufficient Memory**
   ```bash
   # Check memory usage
   free -h
   docker stats
   
   # Solution: Increase Docker memory limit or system RAM
   # For Docker Desktop: Settings > Resources > Memory: 8GB+
   ```

3. **Permission Issues**
   ```bash
   # Check file permissions
   ls -la .env
   ls -la docker-compose.yml
   
   # Fix permissions
   chmod 644 .env
   chmod 644 docker-compose.yml
   
   # Check Docker daemon permissions
   sudo usermod -aG docker $USER
   newgrp docker
   ```

4. **Invalid Configuration**
   ```bash
   # Validate compose file
   ./axiomctl config
   
   # Check environment variables
   ./axiomctl exec api env | grep -E '(DATABASE_URL|REDIS_URL|SECRET_KEY)'
   ```

### Database Connection Issues

#### Symptom: API can't connect to database
**Diagnosis:**
```bash
# Check PostgreSQL container
./axiomctl logs postgres

# Test connection manually
./axiomctl exec postgres psql -U axiom -d axiom -c "SELECT version();"

# Check network connectivity
./axiomctl exec api ping postgres
```

**Solutions:**

1. **Database Not Ready**
   ```bash
   # Wait for database to be ready
   ./axiomctl exec api python -c "
   import time
   from sqlalchemy import create_engine
   import os
   
   engine = create_engine(os.getenv('DATABASE_URL'))
   for i in range(30):
       try:
           engine.connect()
           print('Database ready')
           break
       except:
           print(f'Waiting for database... {i+1}/30')
           time.sleep(2)
   "
   ```

2. **Wrong Connection Parameters**
   ```bash
   # Check DATABASE_URL format
   echo $DATABASE_URL
   # Should be: postgresql://user:password@host:port/database
   
   # Verify in container
   ./axiomctl exec api env | grep DATABASE_URL
   ```

3. **Database Corruption**
   ```bash
   # Backup and recreate database
   ./axiomctl exec postgres pg_dump -U axiom axiom > backup.sql
   ./axiomctl down -v  # WARNING: This deletes data
   ./axiomctl up -d postgres
   ./axiomctl exec postgres psql -U axiom -d axiom < backup.sql
   ```

### Worker Process Issues

#### Symptom: Tasks not processing
**Diagnosis:**
```bash
# Check worker status
./axiomctl logs worker

# Check Celery status
./axiomctl exec api celery -A app.core.celery_app inspect active
./axiomctl exec api celery -A app.core.celery_app inspect reserved

# Check RabbitMQ
./axiomctl exec rabbitmq rabbitmqctl list_queues
```

**Solutions:**

1. **Workers Not Starting**
   ```bash
   # Check worker logs for errors
   ./axiomctl logs worker
   
   # Restart workers
   ./axiomctl restart worker
   
   # Scale workers
   ./axiomctl up -d --scale worker=4
   ```

2. **Task Queue Backed Up**
   ```bash
   # Check queue lengths
   ./axiomctl exec rabbitmq rabbitmqctl list_queues name messages
   
   # Purge queue if needed (development only)
   ./axiomctl exec api celery -A app.core.celery_app purge
   
   # Scale up workers
   ./axiomctl up -d --scale worker=8
   ```

3. **Memory Issues**
   ```bash
   # Check worker memory usage
   docker stats axiom_worker_1
   
   # Set memory limits in docker-compose.yml
   deploy:
     resources:
       limits:
         memory: 4G
   ```

## DICOM Processing Issues

### DICOM Files Not Processing

#### Symptom: Files received but not processed
**Diagnosis:**
```bash
# Check DICOM listener status
./axiomctl exec api python -c "
from app.services.dicom_listener import get_listener_status
print(get_listener_status())
"

# Check recent processing logs
./axiomctl logs api | grep -i "dicom\|process\|rule"

# Check exception logs
curl -H "Authorization: Api-Key your_key" \
  "http://localhost:8001/api/v1/monitoring/exceptions?status=pending"
```

**Solutions:**

1. **Listener Not Running**
   ```bash
   # Check listener configuration
   curl -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/dimse-listeners"
   
   # Restart listener
   ./axiomctl restart api
   ```

2. **Rule Engine Issues**
   ```bash
   # Check active rulesets
   curl -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/rulesets?enabled=true"
   
   # Test rule evaluation
   curl -X POST -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/rulesets/test" \
     -d @test_dicom_data.json
   ```

3. **Storage Backend Issues**
   ```bash
   # Check storage backend status
   curl -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/storage-backends"
   
   # Test storage backend
   curl -X POST -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/storage-backends/test/backend_id"
   ```

### Invalid DICOM Files

#### Symptom: Files rejected with validation errors
**Diagnosis:**
```bash
# Check validation errors
./axiomctl logs api | grep -i "validation\|invalid\|dicom"

# Check specific file
./axiomctl exec api python -c "
import pydicom
ds = pydicom.dcmread('/path/to/file.dcm', force=True)
print('SOPInstanceUID:', ds.get('SOPInstanceUID', 'MISSING'))
print('StudyInstanceUID:', ds.get('StudyInstanceUID', 'MISSING'))
print('Transfer Syntax:', ds.file_meta.get('TransferSyntaxUID', 'MISSING'))
"
```

**Solutions:**

1. **Missing Required Tags**
   ```python
   # Add rule to fix missing tags
   {
     "type": "set",
     "tag": "(0x0008,0x0018)",
     "value": "1.2.840.113619.2.55.3.604688119.971.{timestamp}.{random}"
   }
   ```

2. **Invalid Transfer Syntax**
   ```bash
   # Convert transfer syntax
   ./axiomctl exec api python -c "
   import pydicom
   ds = pydicom.dcmread('/path/to/file.dcm')
   ds.file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
   ds.save_as('/path/to/fixed_file.dcm')
   "
   ```

## Performance Issues

### Slow Processing

#### Symptom: High processing latency
**Diagnosis:**
```bash
# Check processing metrics
curl -H "Authorization: Api-Key your_key" \
  "http://localhost:8001/api/v1/monitoring/stats"

# Check resource usage
docker stats

# Check database performance
./axiomctl exec postgres psql -U axiom -d axiom -c "
SELECT query, mean_exec_time, calls 
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC 
LIMIT 10;"
```

**Solutions:**

1. **Scale Workers**
   ```bash
   # Increase worker count
   ./axiomctl up -d --scale worker=8
   
   # Monitor worker performance
   ./axiomctl exec api celery -A app.core.celery_app inspect stats
   ```

2. **Database Optimization**
   ```sql
   -- Add indexes for common queries
   CREATE INDEX CONCURRENTLY idx_dicom_studies_study_date 
   ON dicom_studies(study_date);
   
   -- Analyze tables
   ANALYZE dicom_studies;
   ```

3. **Rule Engine Optimization**
   ```bash
   # Profile rule execution
   curl -X POST -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/rulesets/profile" \
     -d '{"ruleset_id": "your_ruleset_id"}'
   ```

### Memory Issues

#### Symptom: Out of memory errors
**Diagnosis:**
```bash
# Check memory usage
free -h
docker stats

# Check for memory leaks
./axiomctl exec api python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memory usage: {process.memory_info().rss / 1024 / 1024:.1f} MB')
"
```

**Solutions:**

1. **Increase Memory Limits**
   ```yaml
   # docker-compose.yml
   services:
     api:
       deploy:
         resources:
           limits:
             memory: 4G
   ```

2. **Optimize Worker Memory**
   ```bash
   # Set worker memory limits
   export CELERY_MAX_MEMORY_PER_CHILD=400000  # 400MB
   ./axiomctl restart worker
   ```

## Network and Connectivity Issues

### DICOM Network Issues

#### Symptom: DICOM associations failing
**Diagnosis:**
```bash
# Test DICOM connectivity
./axiomctl exec api python -c "
from pynetdicom import AE
ae = AE()
assoc = ae.associate('target_host', 104)
if assoc.is_established:
    print('DICOM association successful')
    assoc.release()
else:
    print('DICOM association failed')
"

# Check network connectivity
./axiomctl exec api ping target_host
./axiomctl exec api telnet target_host 104
```

**Solutions:**

1. **Firewall Issues**
   ```bash
   # Check firewall rules
   sudo ufw status
   sudo iptables -L
   
   # Open DICOM ports
   sudo ufw allow 11112/tcp
   sudo ufw allow 104/tcp
   ```

2. **AE Title Configuration**
   ```bash
   # Check AE title configuration
   curl -H "Authorization: Api-Key your_key" \
     "http://localhost:8001/api/v1/config/dimse-listeners"
   
   # Verify AE title matches expectations
   ```

### API Connectivity Issues

#### Symptom: API requests timing out
**Diagnosis:**
```bash
# Test API connectivity
curl -v http://localhost:8001/health

# Check API logs
./axiomctl logs api | grep -E "(timeout|error|exception)"

# Check reverse proxy logs
./axiomctl logs nginx | grep -E "(error|timeout)"
```

**Solutions:**

1. **Increase Timeouts**
   ```nginx
   # nginx.conf
   proxy_connect_timeout 60s;
   proxy_send_timeout 300s;
   proxy_read_timeout 300s;
   ```

2. **Scale API Instances**
   ```bash
   ./axiomctl up -d --scale api=3
   ```

## Authentication and Authorization Issues

### Login Issues

#### Symptom: Unable to authenticate
**Diagnosis:**
```bash
# Test Google OAuth
curl -X POST "http://localhost:8001/api/v1/auth/google" \
  -H "Content-Type: application/json" \
  -d '{"token": "your_google_token"}'

# Check API key
curl -H "Authorization: Api-Key your_key" \
  "http://localhost:8001/api/v1/auth/verify"
```

**Solutions:**

1. **Google OAuth Configuration**
   ```bash
   # Verify client ID
   echo $GOOGLE_OAUTH_CLIENT_ID
   
   # Check OAuth scopes and domains
   ```

2. **API Key Issues**
   ```bash
   # Generate new API key
   curl -X POST -H "Authorization: Bearer jwt_token" \
     "http://localhost:8001/api/v1/auth/api-keys" \
     -d '{"name": "new-key", "role": "admin"}'
   ```

## Data and Storage Issues

### Storage Backend Failures

#### Symptom: Files not reaching destinations
**Diagnosis:**
```bash
# Check storage backend status
curl -H "Authorization: Api-Key your_key" \
  "http://localhost:8001/api/v1/config/storage-backends"

# Test storage backend connectivity
curl -X POST -H "Authorization: Api-Key your_key" \
  "http://localhost:8001/api/v1/config/storage-backends/test/backend_id"
```

**Solutions:**

1. **Cloud Storage Issues**
   ```bash
   # Test GCS connectivity
   ./axiomctl exec api python -c "
   from google.cloud import storage
   client = storage.Client()
   bucket = client.bucket('your-bucket')
   print(f'Bucket exists: {bucket.exists()}')
   "
   ```

2. **PACS Connectivity**
   ```bash
   # Test PACS connectivity
   ./axiomctl exec api python -c "
   from pynetdicom import AE
   ae = AE()
   assoc = ae.associate('pacs_host', 104, ae_title='DEST_AE')
   print(f'PACS available: {assoc.is_established}')
   if assoc.is_established:
       assoc.release()
   "
   ```

## Emergency Procedures

### Complete System Reset

⚠️ **WARNING**: This will delete all data. Only use in development or with proper backups.

```bash
# 1. Backup configuration
cp .env .env.backup
cp docker-compose.yml docker-compose.yml.backup

# 2. Stop and remove everything
./axiomctl down -v --remove-orphans

# 3. Clean Docker system
docker system prune -af
docker volume prune -f

# 4. Restart fresh
./axiomctl up -d
./axiomctl exec api alembic upgrade head
./axiomctl exec api python inject_admin.py
```

### Data Recovery

```bash
# Restore from backup
./axiomctl down
./axiomctl up -d postgres
./axiomctl exec postgres psql -U axiom -d axiom < backup.sql
./axiomctl up -d
```

## Getting Additional Help

### Support Channels
1. **GitHub Issues**: For bugs and feature requests
2. **Documentation**: Check the comprehensive docs
3. **Community Forums**: Join discussions with other users
4. **Enterprise Support**: For production deployments

### Information to Include in Support Requests
1. **System Information**: OS, Docker version, hardware specs
2. **Configuration**: Sanitized .env and docker-compose.yml files
3. **Logs**: Output from the log collection script
4. **Error Messages**: Exact error messages and stack traces
5. **Steps to Reproduce**: Detailed reproduction steps

### Log Analysis Tools

```bash
# Real-time log monitoring
./axiomctl logs -f api | grep -E "(ERROR|CRITICAL)"

# Log analysis with jq (for JSON logs)
./axiomctl logs api | jq -r 'select(.level=="ERROR") | .message'

# Performance analysis
./axiomctl logs api | grep "request completed" | tail -100
```

---

This troubleshooting guide covers the most common issues encountered in Axiom deployments. For issues not covered here, collect diagnostic information using the provided scripts and reach out to the support channels.
