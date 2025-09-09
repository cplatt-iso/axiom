# Axiom Backend

The Axiom backend is a high-performance medical imaging processing platform that handles DICOM workflows at enterprise scale. Built with FastAPI and designed for healthcare compliance, it provides intelligent routing, AI-powered standardization, and comprehensive management APIs.

## 🏥 Core Capabilities

### DICOM Data Ingestion
- **C-STORE Reception**: Multi-listener DICOM storage service (SCP) with configurable AE titles
- **DICOMweb STOW-RS**: RESTful DICOM upload endpoint with multipart support
- **Active Polling**: Query DICOMweb sources (QIDO-RS), DIMSE sources (C-FIND), and Google Healthcare APIs
- **Automated Retrieval**: DIMSE C-MOVE and DICOMweb WADO-RS for fetching studies

### Intelligent Processing Engine
- **Rule-Based Processing**: Complex condition matching on DICOM tags and association metadata
- **Tag Morphing**: Modify, add, delete, copy, and move DICOM tags with audit logging
- **AI Standardization**: Leverage OpenAI and Google Vertex AI for tag value standardization
- **Crosswalk Integration**: External database lookups for value mapping and normalization
- **Schedule Management**: Time-based rule activation with timezone support

### Flexible Storage & Routing
- **Multi-Destination Support**: File system, PACS (C-STORE), cloud storage (GCS), DICOMweb endpoints
- **Parallel Routing**: Send processed studies to multiple destinations simultaneously  
- **Storage Policies**: Configure retention, compression, and archival strategies
- **TLS Security**: End-to-end encryption for all DICOM communications

### Enterprise Features
- **Exception Management**: Automated retry with manual intervention capabilities
- **Audit Compliance**: Complete audit trails with original attribute preservation
- **Performance Monitoring**: Real-time metrics, health checks, and alerting
- **Data Browser**: Query and explore data across connected sources
- **User Management**: OAuth 2.0 and API key authentication with RBAC

## 🛠 Technology Stack

- **Runtime**: Python 3.11+ with async/await support
- **Web Framework**: FastAPI with interactive API documentation
- **Database**: PostgreSQL 15+ with SQLAlchemy 2.x ORM
- **Migrations**: Alembic for database schema management
- **Task Processing**: Celery with RabbitMQ message broker
- **Caching**: Redis for session storage and temporary data
- **DICOM Libraries**: Pydicom and Pynetdicom for DICOM operations
- **HTTP Client**: httpx for async HTTP operations
- **Authentication**: Google OAuth 2.0 with JWT token management
- **Cloud Integration**: Google Cloud Storage and Healthcare APIs
- **Logging**: Structured logging with correlation tracing
- **Security**: TLS 1.3 for all communications, encrypted storage

## 🚀 Quick Start

### Prerequisites
- Docker 20.0+ with Docker Compose
- 8GB RAM minimum (16GB recommended for production)
- 50GB free disk space
- Internet connection for initial setup

### Installation

1. **Clone and Configure**
   ```bash
   git clone <repository-url> axiom
   cd axiom/backend
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Start Services**
   ```bash
   ./axiomctl up -d
   ```

3. **Initialize Database**
   ```bash
   ./axiomctl exec api alembic upgrade head
   ./axiomctl exec api python inject_admin.py
   ```

4. **Verify Installation**
   ```bash
   curl http://localhost:8001/health
   # Open http://localhost:8001/api/v1/docs for API documentation
   ```

### Essential Configuration

**Environment Variables (`.env`):**
```bash
# Security (Required)
SECRET_KEY=your_secure_secret_key_here
POSTGRES_PASSWORD=secure_database_password
GOOGLE_OAUTH_CLIENT_ID=your_google_oauth_client_id

# Instance Configuration
AXIOM_INSTANCE_ID=prod
BACKEND_CORS_ORIGINS=https://yourdomain.com

# Performance Tuning
DB_POOL_SIZE=20
CELERY_WORKER_CONCURRENCY=4
REDIS_MAX_CONNECTIONS=100

# Storage Configuration
DICOM_STORAGE_PATH=/app/storage
LOG_ORIGINAL_ATTRIBUTES=true
```

## 📁 Project Structure

```
backend/
├── app/                    # Main application code
│   ├── api/               # REST API endpoints
│   ├── core/              # Core configuration and utilities
│   ├── crud/              # Database operations
│   ├── db/                # Database models and migrations
│   ├── schemas/           # Pydantic models for API
│   ├── services/          # Business logic services
│   └── worker/            # Celery task definitions
├── alembic/               # Database migration scripts
├── docs/                  # Documentation
├── docker/                # Docker configuration files
├── scripts/               # Utility and deployment scripts
├── axiomctl               # Docker Compose wrapper script
└── requirements.txt       # Python dependencies
```

## 📚 Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

### Getting Started
- **[Quick Start](docs/getting-started/quick-start.md)** - Get running in 5 minutes
- **[Installation Guide](docs/getting-started/installation.md)** - Detailed setup for all environments  
- **[Configuration Guide](docs/getting-started/configuration.md)** - Complete configuration reference

### Architecture & Features
- **[System Architecture](docs/architecture/overview.md)** - High-level system design
- **[DICOM Processing](docs/features/dicom-processing.md)** - How medical imaging data flows through Axiom
- **[Rule Engine](docs/features/rule-engine.md)** - Intelligent routing and transformation rules
- **[Storage Backends](docs/features/storage-backends.md)** - Flexible output destinations
- **[AI Integration](docs/features/ai-integration.md)** - AI-powered standardization and automation

### Operations & Development
- **[API Reference](docs/api/)** - Complete REST API documentation
- **[Deployment Guide](docs/operations/deployment.md)** - Production deployment strategies
- **[Monitoring & Troubleshooting](docs/operations/troubleshooting.md)** - Operations and problem resolution
- **[Development Setup](docs/development/setup.md)** - Local development environment

### Quick Links
- **Interactive API Docs**: http://localhost:8001/api/v1/docs (when running)
- **Architecture Diagrams**: [docs/architecture/](docs/architecture/)
- **Configuration Examples**: [docs/getting-started/configuration.md](docs/getting-started/configuration.md)

## 🔧 Basic Configuration Example

**Create a Simple Processing Rule:**
```bash
curl -X POST "http://localhost:8001/api/v1/config/rulesets" \
  -H "Authorization: Api-Key your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ct-chest-routing",
    "execution_mode": "FIRST_MATCH",
    "rules": [{
      "name": "route-ct-chest",
      "conditions": [
        {"type": "tag_equals", "tag": "(0x0008,0x0060)", "value": "CT"},
        {"type": "tag_contains", "tag": "(0x0018,0x0015)", "value": "CHEST"}
      ],
      "actions": [
        {"type": "set", "tag": "(0x0008,0x103E)", "value": "CHEST CT - PROCESSED"}
      ],
      "storage_backends": ["primary-storage"]
    }]
  }'
```

**Send DICOM via STOW-RS:**
```bash
curl -X POST "http://localhost:8001/api/v1/dicomweb/studies" \
  -H "Authorization: Api-Key your_api_key" \
  -H "Content-Type: application/dicom" \
  --data-binary @study.dcm
```

## 🚀 Current Status & Roadmap

### ✅ Production Ready
- **Core DICOM Processing**: C-STORE, STOW-RS, polling sources
- **Rule Engine**: Complete condition and action framework
- **Storage Backends**: File system, cloud storage, PACS integration
- **Authentication & Security**: OAuth 2.0, API keys, RBAC, TLS support
- **Monitoring**: Health checks, metrics, exception management
- **AI Integration**: OpenAI and Google Vertex AI standardization

### 🔄 Active Development
- **Enhanced Exception Handling**: Advanced retry mechanisms and UI management
- **Performance Optimization**: Query optimization and caching improvements
- **Advanced Analytics**: Processing insights and trend analysis
- **Kubernetes Support**: Helm charts and cloud-native deployment

### 🔮 Planned Features
- **Real-time Analytics**: Live processing dashboards and alerts
- **Advanced AI Models**: Custom model integration and training
- **Multi-tenant Support**: Organization-level isolation and management
- **Compliance Extensions**: Additional healthcare standards and certifications

## 🤝 Contributing

We welcome contributions from the medical imaging and healthcare IT community!

### Development Process
1. **Fork the repository** and create a feature branch
2. **Follow our development setup** guide in [docs/development/setup.md](docs/development/setup.md)
3. **Write tests** for new functionality
4. **Ensure code quality** with our pre-commit hooks
5. **Submit a pull request** with a clear description

### Areas for Contribution
- **DICOM Compliance**: Additional SOP classes and transfer syntaxes
- **Healthcare Integrations**: HL7 FHIR, Epic, Cerner connectors
- **AI Models**: Medical imaging specific standardization models
- **Performance**: Optimization for high-volume environments
- **Documentation**: Guides, examples, and tutorials

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support & Community

- **Documentation**: Comprehensive guides in [docs/](docs/)
- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Community Q&A and general discussions
- **Security Issues**: Report to security@yourcompany.com

---

**Axiom - Modern Medical Imaging for Modern Healthcare**

Built with ❤️ by healthcare technologists who believe medical imaging should be as advanced as the rest of the technology stack.
