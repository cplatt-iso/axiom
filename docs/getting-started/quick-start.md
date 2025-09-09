# Quick Start Guide

Get Axiom up and running in 5 minutes. This guide assumes you have Docker installed and are comfortable with command line operations.

## Prerequisites

- Docker 20.0+ with Docker Compose
- 8GB RAM minimum (16GB recommended)
- 50GB free disk space
- Internet connection for container downloads

## 1. Clone and Setup

```bash
# Clone the repository
git clone <your-repo-url> axiom
cd axiom/backend

# Copy environment template
cp .env.example .env
```

## 2. Configure Environment

Edit the `.env` file with your settings:

```bash
# Required changes
POSTGRES_PASSWORD=your_secure_password_here
SECRET_KEY=your_secret_key_here
GOOGLE_OAUTH_CLIENT_ID=your_google_oauth_client_id

# Optional but recommended
BACKEND_CORS_ORIGINS=http://localhost:3000,https://yourdomain.com
AXIOM_INSTANCE_ID=prod  # or dev, staging, etc.
```

**Generate a secret key:**
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

## 3. Start the Platform

```bash
# Start all services
./axiomctl up -d

# Check that services are running
./axiomctl ps
```

You should see containers for:
- `api` - Main FastAPI application
- `worker` - Celery task processor
- `beat` - Celery scheduler
- `postgres` - Database
- `redis` - Cache and message broker
- `rabbitmq` - Task queue

## 4. Initialize Database

```bash
# Run database migrations
./axiomctl exec api alembic upgrade head
```

## 5. Create Your First Admin User

No manual script required! Simply:

1. **Navigate to your Axiom instance** (e.g., `http://localhost:8001`)
2. **Log in with Google OAuth**
3. **You're automatically the admin!** 🎉

The first user to log in receives:
- ✅ Admin role with full privileges  
- ✅ Superuser status
- ✅ Access to all administrative functions

All subsequent users will get standard user permissions by default.

## 6. Verify Installation

### Check API Health
```bash
curl http://localhost:8001/health
```

Should return:
```json
{"status": "healthy", "timestamp": "2025-09-08T10:30:00Z"}
```

### Access API Documentation
Open your browser to: http://localhost:8001/api/v1/docs

You should see the interactive Swagger UI with all available endpoints.

### Test Authentication
```bash
# Get an API key (replace with your actual OAuth token)
curl -X POST "http://localhost:8001/api/v1/auth/api-keys" \
  -H "Authorization: Bearer YOUR_OAUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-key", "role": "admin"}'
```

## 6. Send Your First DICOM

### Option A: Via STOW-RS API
```bash
# Upload a DICOM file
curl -X POST "http://localhost:8001/api/v1/dicomweb/studies" \
  -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: multipart/related; type=application/dicom" \
  --data-binary @path/to/your/dicom/file.dcm
```

### Option B: Via C-STORE
Configure your DICOM modality to send to:
- **AE Title**: `AXIOM`
- **Host**: `localhost`
- **Port**: `11112`

## 7. Basic Configuration

### Create a Storage Backend
```bash
curl -X POST "http://localhost:8001/api/v1/config/storage-backends" \
  -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "local-storage",
    "type": "filesystem",
    "config": {
      "base_path": "/app/storage"
    },
    "enabled": true
  }'
```

### Create a Simple Rule
```bash
curl -X POST "http://localhost:8001/api/v1/config/rulesets" \
  -H "Authorization: Api-Key YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "default-routing",
    "description": "Route all studies to local storage",
    "enabled": true,
    "execution_mode": "FIRST_MATCH",
    "rules": [{
      "name": "route-all",
      "enabled": true,
      "conditions": [],
      "actions": [],
      "storage_backends": ["local-storage"]
    }]
  }'
```

## 🎉 Success!

You now have a fully functional Axiom installation. DICOMs sent to the system will be processed and stored according to your rules.

## Next Steps

1. **[Configure DICOM Sources](../features/dicom-processing.md)** - Set up polling from PACS systems
2. **[Create Complex Rules](../features/rule-engine.md)** - Build sophisticated routing logic
3. **[Set Up Monitoring](../operations/monitoring.md)** - Monitor your deployment
4. **[Production Deployment](../operations/deployment.md)** - Deploy to production

## Troubleshooting

### Services Won't Start
```bash
# Check logs
./axiomctl logs api
./axiomctl logs postgres

# Common issues:
# - Port conflicts (8001, 5432, 6379, 5672)
# - Insufficient disk space
# - Docker daemon not running
```

### Database Connection Errors
```bash
# Reset database
./axiomctl down -v
./axiomctl up -d postgres
# Wait 30 seconds
./axiomctl exec api alembic upgrade head
```

### DICOM Not Processing
```bash
# Check worker logs
./axiomctl logs worker

# Verify task queue
./axiomctl exec api python -c "
from app.core.celery_app import celery_app
print(celery_app.control.inspect().active())
"
```

### Getting Help
- Check the [troubleshooting guide](../operations/troubleshooting.md)
- Review logs: `./axiomctl logs <service>`
- File an issue on GitHub with logs and configuration details

---

**Estimated setup time: 5 minutes**  
**Full configuration time: 30-60 minutes depending on requirements**
