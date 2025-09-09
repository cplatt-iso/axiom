# Production Deployment Guide

This guide covers deploying Axiom to production environments with enterprise-grade security, monitoring, and scalability configurations.

## Deployment Options

### 1. Docker Compose (Small to Medium Scale)
**Recommended for**: Healthcare clinics, small hospitals, pilot deployments  
**Capacity**: Up to 100,000 studies/month  
**Infrastructure**: Single server or small cluster

### 2. Kubernetes (Enterprise Scale)
**Recommended for**: Large hospitals, health systems, enterprise deployments  
**Capacity**: Millions of studies/month  
**Infrastructure**: Kubernetes cluster with auto-scaling

### 3. Cloud Managed Services
**Recommended for**: Rapid deployment, minimal operations overhead  
**Capacity**: Unlimited with proper scaling  
**Infrastructure**: GKE, EKS, or AKS with cloud services

## Pre-Deployment Checklist

### Security Requirements
- [ ] TLS certificates for all endpoints
- [ ] Firewall rules configured
- [ ] VPN access for administrative operations
- [ ] Backup encryption keys secured
- [ ] Security scanning completed
- [ ] Penetration testing performed (if required)

### Infrastructure Requirements
- [ ] Persistent storage provisioned (minimum 1TB)
- [ ] Database backup strategy implemented
- [ ] Monitoring and alerting configured
- [ ] Load balancing configured
- [ ] DNS records configured
- [ ] SSL/TLS termination configured

### Compliance Requirements
- [ ] HIPAA Business Associate Agreement signed
- [ ] Data residency requirements verified
- [ ] Audit logging configured
- [ ] Access controls implemented
- [ ] Incident response plan documented

## Docker Compose Production Deployment

### 1. Server Requirements

**Minimum Production Server:**
```yaml
CPU: 8 cores (16 vCPU)
RAM: 32GB
Storage: 1TB SSD (OS) + 10TB (data)
Network: 1Gbps
OS: Ubuntu 22.04 LTS or RHEL 8
```

**Recommended Production Server:**
```yaml
CPU: 16 cores (32 vCPU)  
RAM: 64GB
Storage: 1TB NVMe (OS) + 50TB (data)
Network: 10Gbps
OS: Ubuntu 22.04 LTS or RHEL 8
```

### 2. System Preparation

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker and Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Configure system limits
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Configure kernel parameters
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
echo "net.core.somaxconn=65535" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### 3. Production Configuration

**Environment Configuration (`.env.prod`):**
```bash
# Production environment
NODE_ENV=production
AXIOM_INSTANCE_ID=prod

# Security
SECRET_KEY=your_very_secure_secret_key_here
POSTGRES_PASSWORD=your_secure_db_password
JWT_SECRET_KEY=your_jwt_secret_key

# Database
DATABASE_URL=postgresql://axiom:${POSTGRES_PASSWORD}@postgres:5432/axiom
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=30

# Redis  
REDIS_URL=redis://redis:6379/0
REDIS_MAX_CONNECTIONS=100

# RabbitMQ
CELERY_BROKER_URL=amqp://axiom:${RABBITMQ_PASSWORD}@rabbitmq:5672//
RABBITMQ_DEFAULT_USER=axiom
RABBITMQ_DEFAULT_PASS=your_rabbitmq_password

# CORS and Security
BACKEND_CORS_ORIGINS=https://axiom.yourdomain.com
SECURE_COOKIES=true
CSRF_PROTECTION=true

# Logging
LOG_LEVEL=INFO
STRUCTURED_LOGGING=true
LOG_FILE=/app/logs/axiom.log

# Storage
DICOM_STORAGE_PATH=/data/dicom
BACKUP_STORAGE_PATH=/data/backups

# Google Cloud (if using)
GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-service-account.json
GCS_BUCKET_NAME=your-axiom-bucket

# Monitoring
SENTRY_DSN=your_sentry_dsn_here
PROMETHEUS_ENABLED=true
```

**Production Docker Compose (`docker-compose.prod.yml`):**
```yaml
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
      - ./nginx/logs:/var/log/nginx
    depends_on:
      - api
    restart: unless-stopped

  api:
    build: .
    env_file: .env.prod
    volumes:
      - ./data:/data
      - ./logs:/app/logs
      - ./secrets:/app/secrets:ro
    depends_on:
      - postgres
      - redis
      - rabbitmq
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G

  worker:
    build: .
    command: celery -A app.core.celery_app worker --loglevel=info --concurrency=4
    env_file: .env.prod
    volumes:
      - ./data:/data
      - ./logs:/app/logs
      - ./secrets:/app/secrets:ro
    depends_on:
      - postgres
      - redis
      - rabbitmq
    restart: unless-stopped
    deploy:
      replicas: 4
      resources:
        limits:
          memory: 8G
        reservations:
          memory: 4G

  beat:
    build: .
    command: celery -A app.core.celery_app beat --loglevel=info
    env_file: .env.prod
    volumes:
      - ./data:/data
      - ./logs:/app/logs
    depends_on:
      - postgres
      - redis
      - rabbitmq
    restart: unless-stopped

  postgres:
    image: postgres:15
    env_file: .env.prod
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./backups:/backups
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 8G
        reservations:
          memory: 4G

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes --maxmemory 2gb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    restart: unless-stopped

  rabbitmq:
    image: rabbitmq:3-management-alpine
    env_file: .env.prod
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    ports:
      - "15672:15672"  # Management UI (restrict access)
    restart: unless-stopped

volumes:
  postgres_data:
  redis_data:
  rabbitmq_data:
```

### 4. SSL/TLS Configuration

**Nginx Configuration (`nginx/nginx.conf`):**
```nginx
upstream axiom_api {
    server api:8000;
}

server {
    listen 80;
    server_name axiom.yourdomain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name axiom.yourdomain.com;

    ssl_certificate /etc/nginx/ssl/axiom.crt;
    ssl_certificate_key /etc/nginx/ssl/axiom.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    add_header X-XSS-Protection "1; mode=block" always;

    # File upload limits for DICOM
    client_max_body_size 2G;
    client_body_timeout 300s;

    location / {
        proxy_pass http://axiom_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts for large DICOM uploads
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }

    location /health {
        proxy_pass http://axiom_api/health;
        access_log off;
    }
}
```

### 5. Backup Strategy

**Database Backup Script (`scripts/backup-db.sh`):**
```bash
#!/bin/bash
BACKUP_DIR="/data/backups"
DATE=$(date +%Y%m%d_%H%M%S)
CONTAINER_NAME="axiom_postgres_1"

# Create backup directory
mkdir -p $BACKUP_DIR

# Database backup
docker exec $CONTAINER_NAME pg_dump -U axiom axiom | gzip > $BACKUP_DIR/axiom_db_$DATE.sql.gz

# Keep only last 30 days of backups
find $BACKUP_DIR -name "axiom_db_*.sql.gz" -mtime +30 -delete

# Upload to cloud storage (optional)
if [ ! -z "$BACKUP_BUCKET" ]; then
    gsutil cp $BACKUP_DIR/axiom_db_$DATE.sql.gz gs://$BACKUP_BUCKET/backups/
fi
```

**Automated Backup Cron:**
```bash
# Add to crontab: crontab -e
0 2 * * * /opt/axiom/scripts/backup-db.sh
0 6 * * 0 /opt/axiom/scripts/backup-files.sh  # Weekly file backup
```

### 6. Monitoring Setup

**Prometheus Configuration (`monitoring/prometheus.yml`):**
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'axiom-api'
    static_configs:
      - targets: ['api:8000']
    metrics_path: '/metrics'

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']

  - job_name: 'rabbitmq'
    static_configs:
      - targets: ['rabbitmq:15692']
```

## Kubernetes Deployment

### 1. Namespace and RBAC

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: axiom-prod
  labels:
    name: axiom-prod
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: axiom-service-account
  namespace: axiom-prod
```

### 2. ConfigMap and Secrets

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: axiom-config
  namespace: axiom-prod
data:
  AXIOM_INSTANCE_ID: "prod"
  LOG_LEVEL: "INFO"
  STRUCTURED_LOGGING: "true"
  PROMETHEUS_ENABLED: "true"
---
apiVersion: v1
kind: Secret
metadata:
  name: axiom-secrets
  namespace: axiom-prod
type: Opaque
data:
  SECRET_KEY: <base64-encoded-secret>
  POSTGRES_PASSWORD: <base64-encoded-password>
  JWT_SECRET_KEY: <base64-encoded-jwt-secret>
```

### 3. Database Deployment

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: axiom-prod
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15
        env:
        - name: POSTGRES_DB
          value: axiom
        - name: POSTGRES_USER
          value: axiom
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: axiom-secrets
              key: POSTGRES_PASSWORD
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
  volumeClaimTemplates:
  - metadata:
      name: postgres-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

### 4. API Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: axiom-api
  namespace: axiom-prod
spec:
  replicas: 3
  selector:
    matchLabels:
      app: axiom-api
  template:
    metadata:
      labels:
        app: axiom-api
    spec:
      serviceAccountName: axiom-service-account
      containers:
      - name: api
        image: axiom:latest
        ports:
        - containerPort: 8000
        env:
        - name: SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: axiom-secrets
              key: SECRET_KEY
        envFrom:
        - configMapRef:
            name: axiom-config
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
```

### 5. Ingress Configuration

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: axiom-ingress
  namespace: axiom-prod
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/proxy-body-size: "2g"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  tls:
  - hosts:
    - axiom.yourdomain.com
    secretName: axiom-tls
  rules:
  - host: axiom.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: axiom-api-service
            port:
              number: 80
```

## Post-Deployment Verification

### 1. Health Checks
```bash
# Test API health
curl -k https://axiom.yourdomain.com/health

# Test authentication
curl -X POST https://axiom.yourdomain.com/api/v1/auth/api-keys \
  -H "Authorization: Bearer YOUR_OAUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-key", "role": "admin"}'

# Test DICOM endpoint
curl -X POST https://axiom.yourdomain.com/api/v1/dicomweb/studies \
  -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: application/dicom" \
  --data-binary @test.dcm
```

### 2. Load Testing
```bash
# Install k6 load testing tool
sudo apt install k6

# Run load test
k6 run --vus 10 --duration 30s load-test.js
```

### 3. Security Verification
```bash
# SSL configuration test
nmap --script ssl-enum-ciphers -p 443 axiom.yourdomain.com

# Security headers test
curl -I https://axiom.yourdomain.com

# Vulnerability scan (if permitted)
nikto -h https://axiom.yourdomain.com
```

## Scaling Considerations

### Horizontal Scaling
- **API Instances**: Scale based on request volume
- **Worker Processes**: Scale based on processing queue length
- **Database**: Use read replicas for query performance

### Vertical Scaling
- **Memory**: Increase for large DICOM file processing
- **CPU**: Increase for intensive AI processing operations
- **Storage**: Use fast SSDs for active data, cheaper storage for archives

### Auto-scaling Configuration
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: axiom-api-hpa
  namespace: axiom-prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: axiom-api
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## Disaster Recovery

### Backup Verification
```bash
# Test database restore
docker run --rm -v $(pwd)/backups:/backups postgres:15 \
  psql -h target_host -U axiom -d axiom -f /backups/latest_backup.sql

# Verify data integrity
./scripts/verify-backup.sh latest_backup.sql
```

### Recovery Procedures
1. **Database Recovery**: Restore from latest backup
2. **File Recovery**: Restore DICOM files from storage backup
3. **Configuration Recovery**: Restore from version control
4. **Service Recovery**: Redeploy from known good images

---

This deployment guide provides enterprise-grade deployment configurations with security, monitoring, and scalability built in. Adjust configurations based on your specific infrastructure and compliance requirements.
