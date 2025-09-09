# Installation Guide

This guide provides detailed installation instructions for Axiom in various environments, from development setups to production deployments.

## Development Installation

### System Requirements

**Minimum Development Environment:**
- CPU: 4 cores
- RAM: 8GB
- Storage: 50GB free space
- OS: Linux, macOS, or Windows with WSL2

**Recommended Development Environment:**
- CPU: 8 cores
- RAM: 16GB
- Storage: 100GB SSD
- OS: Ubuntu 22.04 LTS or macOS 12+

### Prerequisites Installation

#### Docker & Docker Compose
```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# macOS (via Homebrew)
brew install docker docker-compose
```

#### Python Development Environment (Optional)
```bash
# Install Python 3.11+
sudo apt install python3.11 python3.11-venv python3.11-dev

# macOS
brew install python@3.11

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate
```

### Development Setup

#### 1. Clone Repository
```bash
git clone https://github.com/your-org/axiom.git
cd axiom/backend
```

#### 2. Environment Configuration
```bash
# Copy development environment template
cp .env.example .env.dev

# Edit configuration for development
cat > .env.dev << EOF
# Development Configuration
NODE_ENV=development
AXIOM_INSTANCE_ID=dev
DEBUG=true

# Database
POSTGRES_DB=axiom_dev
POSTGRES_USER=axiom
POSTGRES_PASSWORD=dev_password_123
DATABASE_URL=postgresql://axiom:dev_password_123@localhost:5432/axiom_dev

# Security (Development Only)
SECRET_KEY=dev_secret_key_not_for_production
JWT_SECRET_KEY=dev_jwt_secret_key
SECURE_COOKIES=false

# CORS for local frontend
BACKEND_CORS_ORIGINS=http://localhost:3000,http://localhost:8080

# Logging
LOG_LEVEL=DEBUG
STRUCTURED_LOGGING=true

# Storage
DICOM_STORAGE_PATH=./dev_data/dicom
BACKUP_STORAGE_PATH=./dev_data/backups

# Disable external services for development
GOOGLE_OAUTH_CLIENT_ID=fake_client_id
SENTRY_DSN=
EOF
```

#### 3. Development Compose Setup
```bash
# Create development compose override
cat > docker-compose.dev.yml << 'EOF'
version: '3.8'

services:
  api:
    build:
      context: .
      dockerfile: Dockerfile
      target: development
    volumes:
      - ./app:/app/app
      - ./alembic:/app/alembic
      - ./scripts:/app/scripts
      - ./dev_data:/data
    ports:
      - "8001:8000"
    env_file: .env.dev
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    depends_on:
      - postgres
      - redis
      - rabbitmq

  worker:
    build:
      context: .
      dockerfile: Dockerfile
      target: development
    volumes:
      - ./app:/app/app
      - ./dev_data:/data
    env_file: .env.dev
    command: celery -A app.core.celery_app worker --loglevel=debug --concurrency=2
    depends_on:
      - postgres
      - redis
      - rabbitmq

  beat:
    build:
      context: .
      dockerfile: Dockerfile
      target: development
    volumes:
      - ./app:/app/app
      - ./dev_data:/data
    env_file: .env.dev
    command: celery -A app.core.celery_app beat --loglevel=debug
    depends_on:
      - postgres
      - redis
      - rabbitmq

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: axiom_dev
      POSTGRES_USER: axiom
      POSTGRES_PASSWORD: dev_password_123
    ports:
      - "5432:5432"
    volumes:
      - postgres_dev_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  rabbitmq:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: dev
      RABBITMQ_DEFAULT_PASS: dev

volumes:
  postgres_dev_data:
EOF
```

#### 4. Start Development Environment
```bash
# Start services
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Check services are running
docker-compose ps

# Initialize database
docker-compose exec api alembic upgrade head

# No admin script needed! First user becomes admin automatically
```

#### 5. Create Your First Admin User

Instead of running a script, simply:

1. **Start the frontend/login interface** (if available)
2. **Log in with Google OAuth**
3. **First user automatically gets admin privileges!**

The system will automatically:
- ✅ Assign Admin role to the first user
- ✅ Grant superuser status  
- ✅ Enable full administrative access

#### 6. Verify Development Setup
```bash
# Test API health
curl http://localhost:8001/health

# Test API documentation
open http://localhost:8001/api/v1/docs

# Check worker logs
docker-compose logs -f worker

# Access RabbitMQ management
open http://localhost:15672  # dev/dev
```

## Local Python Development

For rapid development and debugging, you can run the API locally while using Docker for dependencies.

### 1. Install Dependencies
```bash
# Activate virtual environment
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Start Dependencies Only
```bash
# Start only databases and message queue
docker-compose -f docker-compose.dev.yml up -d postgres redis rabbitmq
```

### 3. Run API Locally
```bash
# Set environment variables
export $(cat .env.dev | xargs)
export DATABASE_URL=postgresql://axiom:dev_password_123@localhost:5432/axiom_dev

# Run database migrations
alembic upgrade head

# Start API with hot reload
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# In another terminal, start worker
celery -A app.core.celery_app worker --loglevel=debug

# In another terminal, start beat scheduler
celery -A app.core.celery_app beat --loglevel=debug
```

## Testing Environment Setup

### Unit Testing
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Create test database
createdb -h localhost -U axiom axiom_test

# Set test environment
export DATABASE_URL=postgresql://axiom:dev_password_123@localhost:5432/axiom_test
export TESTING=true

# Run tests
pytest app/tests/ -v --cov=app --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Integration Testing
```bash
# Start test environment
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
pytest app/tests/integration/ -v

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

## Production Installation

### Server Requirements

**Minimum Production Server:**
```yaml
CPU: 8 cores (16 vCPU)
RAM: 32GB
Storage: 1TB SSD (OS) + 10TB (data)
Network: 1Gbps
OS: Ubuntu 22.04 LTS or RHEL 8+
```

**Recommended Production Server:**
```yaml
CPU: 16 cores (32 vCPU)
RAM: 64GB
Storage: 2TB NVMe (OS) + 50TB (data)
Network: 10Gbps
OS: Ubuntu 22.04 LTS
```

### 1. System Preparation
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install essential packages
sudo apt install -y curl wget gnupg2 software-properties-common apt-transport-https ca-certificates

# Configure system limits
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "root soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "root hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Configure kernel parameters
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
echo "net.core.somaxconn=65535" | sudo tee -a /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog=65535" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

# Create application user
sudo useradd -m -s /bin/bash axiom
sudo usermod -aG docker axiom
```

### 2. Install Docker
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Configure Docker daemon
sudo mkdir -p /etc/docker
cat << EOF | sudo tee /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "userland-proxy": false,
  "experimental": false
}
EOF

sudo systemctl restart docker
sudo systemctl enable docker
```

### 3. Application Deployment
```bash
# Switch to application user
sudo su - axiom

# Clone repository
git clone https://github.com/your-org/axiom.git
cd axiom/backend

# Create production directories
sudo mkdir -p /opt/axiom/{data,logs,backups,secrets}
sudo chown -R axiom:axiom /opt/axiom

# Create production environment
cp .env.example .env.prod
# Edit .env.prod with production values

# Deploy with production compose
./axiomctl -f docker-compose.prod.yml up -d

# Initialize database
./axiomctl exec api alembic upgrade head

# No inject_admin script needed! 
# First user to log in becomes admin automatically
```

**🎉 Admin User Creation**: The first user to log in via Google OAuth will automatically receive admin privileges. No manual script execution required!

### 4. SSL/TLS Setup
```bash
# Install Certbot for Let's Encrypt
sudo apt install certbot python3-certbot-nginx

# Generate SSL certificate
sudo certbot --nginx -d axiom.yourdomain.com

# Setup auto-renewal
sudo crontab -e
# Add: 0 12 * * * /usr/bin/certbot renew --quiet
```

### 5. Backup Configuration
```bash
# Create backup script
cat > /opt/axiom/scripts/backup.sh << 'EOF'
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/opt/axiom/backups"

# Database backup
docker exec axiom_postgres_1 pg_dump -U axiom axiom | gzip > $BACKUP_DIR/axiom_db_$DATE.sql.gz

# Configuration backup
tar -czf $BACKUP_DIR/axiom_config_$DATE.tar.gz /opt/axiom/backend/.env.prod /opt/axiom/backend/docker-compose.prod.yml

# Keep only last 30 days
find $BACKUP_DIR -name "*.gz" -mtime +30 -delete

echo "Backup completed: $DATE"
EOF

chmod +x /opt/axiom/scripts/backup.sh

# Setup cron job
crontab -e
# Add: 0 2 * * * /opt/axiom/scripts/backup.sh >> /opt/axiom/logs/backup.log 2>&1
```

## Cloud Installation

### Google Cloud Platform

#### 1. GKE Cluster Setup
```bash
# Create GKE cluster
gcloud container clusters create axiom-prod \
  --zone=us-central1-a \
  --machine-type=n1-standard-4 \
  --num-nodes=3 \
  --enable-autorepair \
  --enable-autoupgrade \
  --enable-autoscaling \
  --min-nodes=2 \
  --max-nodes=10

# Get credentials
gcloud container clusters get-credentials axiom-prod --zone=us-central1-a
```

#### 2. Deploy with Helm
```bash
# Install Helm
curl https://get.helm.sh/helm-v3.12.0-linux-amd64.tar.gz | tar xz
sudo mv linux-amd64/helm /usr/local/bin/

# Deploy Axiom
helm install axiom ./helm/axiom \
  --namespace axiom-prod \
  --create-namespace \
  --values helm/values-prod.yaml
```

### AWS EKS

#### 1. EKS Cluster Setup
```bash
# Install eksctl
curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz
sudo mv eksctl /usr/local/bin

# Create cluster
eksctl create cluster --name axiom-prod --version 1.27 --region us-west-2 --nodegroup-name standard-workers --node-type m5.xlarge --nodes 3 --nodes-min 2 --nodes-max 10
```

#### 2. Deploy Application
```bash
# Deploy with kubectl
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/rabbitmq.yaml
kubectl apply -f k8s/axiom-api.yaml
kubectl apply -f k8s/axiom-worker.yaml
kubectl apply -f k8s/ingress.yaml
```

## Troubleshooting Installation

### Common Issues

#### Docker Permission Errors
```bash
# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Test docker access
docker run hello-world
```

#### Database Connection Issues
```bash
# Check PostgreSQL status
docker-compose logs postgres

# Test database connection
docker-compose exec postgres psql -U axiom -d axiom -c "SELECT version();"

# Reset database
docker-compose down -v
docker-compose up -d postgres
# Wait 30 seconds
docker-compose exec api alembic upgrade head
```

#### Memory Issues
```bash
# Check memory usage
docker stats

# Increase Docker memory limit (Docker Desktop)
# Settings > Resources > Advanced > Memory: 8GB+

# Linux memory optimization
echo 'vm.swappiness=10' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

#### Port Conflicts
```bash
# Check port usage
sudo netstat -tulpn | grep :8001
sudo lsof -i :8001

# Kill conflicting processes
sudo kill -9 $(sudo lsof -t -i:8001)

# Use different ports in docker-compose.yml
ports:
  - "8002:8000"  # Use 8002 instead of 8001
```

### Verification Steps

#### Health Check Script
```bash
#!/bin/bash
# health-check.sh

echo "Checking Axiom installation..."

# API Health
if curl -f http://localhost:8001/health > /dev/null 2>&1; then
    echo "✓ API is healthy"
else
    echo "✗ API is not responding"
    exit 1
fi

# Database Check
if docker-compose exec -T postgres psql -U axiom -d axiom -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✓ Database is accessible"
else
    echo "✗ Database connection failed"
    exit 1
fi

# Worker Check
ACTIVE_WORKERS=$(docker-compose exec -T api python -c "
from app.core.celery_app import celery_app
import json
inspect = celery_app.control.inspect()
active = inspect.active()
print(len(active) if active else 0)
" 2>/dev/null)

if [ "$ACTIVE_WORKERS" -gt 0 ]; then
    echo "✓ Workers are active ($ACTIVE_WORKERS)"
else
    echo "✗ No active workers found"
fi

echo "Installation verification complete!"
```

### Getting Help

If you encounter issues not covered here:

1. **Check Logs**: Use `docker-compose logs service_name` to view service logs
2. **Review Configuration**: Verify `.env` file settings
3. **Check Resources**: Ensure adequate CPU, memory, and disk space
4. **Network Issues**: Verify firewall and port configurations
5. **File an Issue**: Create a GitHub issue with logs and configuration details

---

This installation guide covers development, testing, and production deployments. Choose the appropriate section based on your use case and environment requirements.
