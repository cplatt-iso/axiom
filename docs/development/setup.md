# Development Setup

This guide walks you through setting up a complete development environment for Axiom backend development, including IDE configuration, debugging tools, and testing frameworks.

## Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows with WSL2
- **CPU**: 4+ cores recommended
- **RAM**: 16GB recommended (8GB minimum)
- **Storage**: 50GB free space
- **Docker**: Version 20.0+
- **Python**: 3.11+ for local development

### Development Tools
- **Git**: Version control
- **VS Code** or **PyCharm**: IDE with Python support
- **Docker Desktop**: Container management
- **Postman** or **httpie**: API testing

## Environment Setup

### 1. Clone and Initial Setup

```bash
# Clone the repository
git clone https://github.com/your-org/axiom.git
cd axiom/backend

# Create development branch
git checkout -b feature/your-feature-name

# Copy development environment
cp .env.example .env.dev
```

### 2. Development Environment Configuration

Edit `.env.dev` with development-specific settings:

```bash
# Development Environment Configuration
NODE_ENV=development
AXIOM_INSTANCE_ID=dev
DEBUG=true

# Database (development)
POSTGRES_DB=axiom_dev
POSTGRES_USER=axiom_dev
POSTGRES_PASSWORD=dev_password_123
DATABASE_URL=postgresql://axiom_dev:dev_password_123@localhost:5432/axiom_dev

# Security (development only - not for production)
SECRET_KEY=dev_secret_key_change_in_production
JWT_SECRET_KEY=dev_jwt_secret_not_for_production
SECURE_COOKIES=false
CSRF_PROTECTION=false

# CORS for local development
BACKEND_CORS_ORIGINS=http://localhost:3000,http://localhost:8080,http://localhost:3001

# Logging
LOG_LEVEL=DEBUG
STRUCTURED_LOGGING=true
LOG_TO_CONSOLE=true

# Storage paths
DICOM_STORAGE_PATH=./dev_data/dicom
TEMP_STORAGE_PATH=./dev_data/temp
BACKUP_STORAGE_PATH=./dev_data/backups

# Disable external services for development
GOOGLE_OAUTH_CLIENT_ID=fake_dev_client_id
SENTRY_DSN=
AI_ENABLED=false
PROMETHEUS_ENABLED=false

# Worker settings for development
CELERY_TASK_ALWAYS_EAGER=false  # Set to true for synchronous testing
CELERY_WORKER_CONCURRENCY=2

# Development features
AUTO_RELOAD=true
PROFILING_ENABLED=true
SQL_ECHO=false  # Set to true to log all SQL queries
```

### 3. Docker Development Setup

Create `docker-compose.dev.yml` for development:

```yaml
version: '3.8'

services:
  api:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      # Source code hot-reload
      - ./app:/app/app
      - ./alembic:/app/alembic
      - ./scripts:/app/scripts
      - ./tests:/app/tests
      # Development data
      - ./dev_data:/data
      # Python cache for faster rebuilds
      - dev_python_cache:/home/developer/.cache/pip
    ports:
      - "8001:8000"
      - "5678:5678"  # Debug port
    env_file: .env.dev
    environment:
      - PYTHONPATH=/app
      - WATCHDOG_POLL=true
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --log-level debug
    depends_on:
      - postgres-dev
      - redis-dev
      - rabbitmq-dev

  worker:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      - ./app:/app/app
      - ./dev_data:/data
      - dev_python_cache:/home/developer/.cache/pip
    env_file: .env.dev
    environment:
      - PYTHONPATH=/app
    command: celery -A app.core.celery_app worker --loglevel=debug --concurrency=2 --pool=solo
    depends_on:
      - postgres-dev
      - redis-dev
      - rabbitmq-dev

  beat:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      - ./app:/app/app
      - ./dev_data:/data
    env_file: .env.dev
    environment:
      - PYTHONPATH=/app
    command: celery -A app.core.celery_app beat --loglevel=debug
    depends_on:
      - postgres-dev
      - redis-dev
      - rabbitmq-dev

  postgres-dev:
    image: postgres:15
    environment:
      POSTGRES_DB: axiom_dev
      POSTGRES_USER: axiom_dev
      POSTGRES_PASSWORD: dev_password_123
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --lc-collate=C --lc-ctype=C"
    ports:
      - "5433:5432"  # Different port to avoid conflicts
    volumes:
      - postgres_dev_data:/var/lib/postgresql/data
      - ./scripts/dev_init.sql:/docker-entrypoint-initdb.d/init.sql

  redis-dev:
    image: redis:7-alpine
    ports:
      - "6380:6379"  # Different port
    command: redis-server --appendonly yes --maxmemory 512mb

  rabbitmq-dev:
    image: rabbitmq:3-management-alpine
    ports:
      - "5673:5672"   # AMQP port
      - "15673:15672" # Management UI
    environment:
      RABBITMQ_DEFAULT_USER: dev
      RABBITMQ_DEFAULT_PASS: dev
      RABBITMQ_DEFAULT_VHOST: axiom_dev

  # Optional: Local test DICOM server
  orthanc-dev:
    image: orthancteam/orthanc:latest
    ports:
      - "4242:4242"   # DICOM port
      - "8042:8042"   # Web UI
    environment:
      ORTHANC__NAME: "Axiom Dev PACS"
      ORTHANC__DICOM_AET: "DEV_PACS"
      ORTHANC__DICOM_PORT: 4242
    volumes:
      - orthanc_dev_data:/var/lib/orthanc/db

volumes:
  postgres_dev_data:
  orthanc_dev_data:
  dev_python_cache:
```

### 4. Development Dockerfile

Create `Dockerfile.dev`:

```dockerfile
FROM python:3.11-slim

# Development user
RUN useradd -m -s /bin/bash developer && \
    apt-get update && \
    apt-get install -y \
    build-essential \
    curl \
    git \
    vim \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Development tools
RUN pip install debugpy ipython ipdb

# Switch to development user
USER developer

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose debug port
EXPOSE 5678

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
```

## Local Python Development

For faster development cycles, you can run the API locally while using Docker for dependencies.

### 1. Python Virtual Environment

```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Development Dependencies

Add these to `requirements-dev.txt`:

```txt
# Testing
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0
pytest-xdist>=3.3.0
httpx[test]>=0.24.0

# Development tools
black>=23.7.0
isort>=5.12.0
flake8>=6.0.0
mypy>=1.5.0
pre-commit>=3.3.0

# Debugging
debugpy>=1.6.0
ipython>=8.14.0
ipdb>=0.13.0

# Documentation
mkdocs>=1.5.0
mkdocs-material>=9.1.0

# Database management
alembic-utils>=0.8.0

# Performance profiling
py-spy>=0.3.0
memory-profiler>=0.60.0
```

### 3. Start Dependencies Only

```bash
# Start only databases and message queue
docker-compose -f docker-compose.dev.yml up -d postgres-dev redis-dev rabbitmq-dev
```

### 4. Run API Locally

```bash
# Set environment variables
export $(cat .env.dev | xargs)
export DATABASE_URL=postgresql://axiom_dev:dev_password_123@localhost:5433/axiom_dev
export REDIS_URL=redis://localhost:6380/0
export CELERY_BROKER_URL=amqp://dev:dev@localhost:5673/axiom_dev

# Run database migrations
alembic upgrade head

# Start API with hot reload
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --log-level debug

# In separate terminals:
# Start worker
celery -A app.core.celery_app worker --loglevel=debug --pool=solo

# Start beat scheduler
celery -A app.core.celery_app beat --loglevel=debug
```

## IDE Configuration

### VS Code Setup

Install recommended extensions:
```json
{
  "recommendations": [
    "ms-python.python",
    "ms-python.black-formatter",
    "ms-python.isort",
    "ms-python.flake8",
    "ms-python.mypy-type-checker",
    "ms-python.debugpy",
    "charliermarsh.ruff",
    "tamasfe.even-better-toml",
    "redhat.vscode-yaml",
    "ms-vscode.docker"
  ]
}
```

Configure settings in `.vscode/settings.json`:
```json
{
  "python.defaultInterpreterPath": "./venv/bin/python",
  "python.terminal.activateEnvironment": true,
  "python.linting.enabled": true,
  "python.linting.flake8Enabled": true,
  "python.formatting.provider": "black",
  "python.sortImports.args": ["--profile", "black"],
  "editor.formatOnSave": true,
  "editor.codeActionsOnSave": {
    "source.organizeImports": true
  },
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["tests/"],
  "files.exclude": {
    "**/__pycache__": true,
    "**/*.pyc": true
  }
}
```

Create debug configuration in `.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "FastAPI Server",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/venv/bin/uvicorn",
      "args": ["app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"],
      "env": {
        "PYTHONPATH": "${workspaceFolder}",
        "DATABASE_URL": "postgresql://axiom_dev:dev_password_123@localhost:5433/axiom_dev"
      },
      "console": "integratedTerminal"
    },
    {
      "name": "Celery Worker",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/venv/bin/celery",
      "args": ["-A", "app.core.celery_app", "worker", "--loglevel=debug", "--pool=solo"],
      "env": {
        "PYTHONPATH": "${workspaceFolder}"
      },
      "console": "integratedTerminal"
    },
    {
      "name": "Attach to Docker API",
      "type": "python",
      "request": "attach",
      "connect": {
        "host": "localhost",
        "port": 5678
      },
      "pathMappings": [
        {
          "localRoot": "${workspaceFolder}",
          "remoteRoot": "/app"
        }
      ]
    }
  ]
}
```

### PyCharm Setup

1. **Configure Python Interpreter**:
   - File → Settings → Project → Python Interpreter
   - Add Local Interpreter → Existing Environment
   - Select `./venv/bin/python`

2. **Configure Run Configurations**:
   ```
   Script: uvicorn
   Parameters: app.main:app --host 0.0.0.0 --port 8000 --reload
   Environment: DATABASE_URL=postgresql://...
   Working Directory: /path/to/axiom/backend
   ```

3. **Configure Testing**:
   - Settings → Tools → Python Integrated Tools
   - Default test runner: pytest
   - Test root: tests/

## Code Quality Tools

### Pre-commit Hooks

Install pre-commit hooks to ensure code quality:

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install
```

Create `.pre-commit-config.yaml`:
```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-json
      - id: check-merge-conflict

  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: ["--profile", "black"]

  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        additional_dependencies: [flake8-docstrings]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.5.0
    hooks:
      - id: mypy
        additional_dependencies: [types-requests]
```

### Code Formatting

Configure formatting tools:

```bash
# Black configuration in pyproject.toml
[tool.black]
line-length = 100
target-version = ['py311']
include = '\.pyi?$'

# isort configuration
[tool.isort]
profile = "black"
line_length = 100
known_first_party = ["app"]

# flake8 configuration in .flake8
[flake8]
max-line-length = 100
ignore = E203, W503
exclude = .git,__pycache__,venv,migrations
```

## Testing Setup

### Test Structure

Organize tests in the `tests/` directory:

```
tests/
├── conftest.py              # Pytest configuration
├── unit/                    # Unit tests
│   ├── test_models.py
│   ├── test_services.py
│   └── test_utils.py
├── integration/             # Integration tests
│   ├── test_api.py
│   ├── test_database.py
│   └── test_celery.py
├── e2e/                     # End-to-end tests
│   ├── test_dicom_workflow.py
│   └── test_rule_engine.py
└── fixtures/                # Test data
    ├── dicom_files/
    └── sample_data.json
```

### Test Configuration

Create `tests/conftest.py`:

```python
import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from app.main import app
from app.core.config import settings
from app.db.session import get_db
from app.db.base import Base

# Test database setup
@pytest.fixture(scope="session")
def db_container():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres

@pytest.fixture(scope="session")
def test_db_url(db_container):
    return db_container.get_connection_url()

@pytest.fixture(scope="session")
def test_engine(test_db_url):
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)

@pytest.fixture
def test_db_session(test_engine):
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.rollback()
    session.close()

@pytest.fixture
def override_get_db(test_db_session):
    def _override_get_db():
        yield test_db_session
    return _override_get_db

@pytest.fixture
async def client(override_get_db):
    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()

# DICOM test data
@pytest.fixture
def sample_dicom_dataset():
    from pydicom.dataset import Dataset
    ds = Dataset()
    ds.SOPInstanceUID = "1.2.840.113619.2.55.3.604688119.971.1234567890.123"
    ds.StudyInstanceUID = "1.2.840.113619.2.55.3.604688119.971.1234567890"
    ds.SeriesInstanceUID = "1.2.840.113619.2.55.3.604688119.971.123456789"
    ds.PatientID = "TEST123"
    ds.PatientName = "Test^Patient"
    ds.Modality = "CT"
    ds.StudyDate = "20250909"
    return ds
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
pytest tests/e2e/

# Run tests in parallel
pytest -n auto

# Run with verbose output
pytest -v

# Run specific test
pytest tests/unit/test_services.py::test_rule_engine
```

## Debugging

### Python Debugger

Use the built-in debugger:

```python
import pdb; pdb.set_trace()  # Standard debugger

import ipdb; ipdb.set_trace()  # Enhanced debugger (if installed)

# In async functions
import asyncio
await asyncio.sleep(0)  # Allow other tasks to run
import pdb; pdb.set_trace()
```

### Remote Debugging

For debugging in Docker containers:

```python
# Add to your code
import debugpy
debugpy.listen(("0.0.0.0", 5678))
debugpy.wait_for_client()  # Optional: wait for debugger to attach
```

Then connect from VS Code using the "Attach to Docker API" configuration.

### Logging for Development

Enhanced logging for development:

```python
import logging
from app.core.config import settings

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Use in code
logger = logging.getLogger(__name__)
logger.debug("Detailed debug information")
logger.info("General information")
logger.warning("Warning message")
logger.error("Error occurred", exc_info=True)
```

## Performance Profiling

### API Performance

Profile API endpoints:

```python
# Install py-spy
pip install py-spy

# Profile running process
py-spy record -o profile.svg -d 60 -p $(pgrep -f uvicorn)

# Memory profiling
from memory_profiler import profile

@profile
def memory_intensive_function():
    # Your code here
    pass
```

### Database Performance

Monitor database queries:

```python
# Enable SQL logging in development
import logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Use EXPLAIN ANALYZE for slow queries
from sqlalchemy import text
result = session.execute(text("EXPLAIN ANALYZE SELECT ..."))
```

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/new-feature

# Start development environment
docker-compose -f docker-compose.dev.yml up -d

# Make changes and test
# ... development work ...

# Run tests
pytest

# Check code quality
black --check .
isort --check-only .
flake8 .
mypy .
```

### 2. Database Changes

```bash
# Create migration
alembic revision --autogenerate -m "Add new table"

# Review migration file
# Edit if needed

# Apply migration
alembic upgrade head

# Test rollback
alembic downgrade -1
alembic upgrade head
```

### 3. API Changes

```bash
# Update Pydantic models in app/schemas/
# Update CRUD operations in app/crud/
# Update API endpoints in app/api/

# Test API changes
pytest tests/integration/test_api.py

# Check OpenAPI docs
curl http://localhost:8001/api/v1/openapi.json
```

## Common Development Tasks

### Adding New API Endpoint

1. **Define Pydantic models** in `app/schemas/`
2. **Add database model** in `app/db/models/`
3. **Create CRUD operations** in `app/crud/`
4. **Add API endpoint** in `app/api/endpoints/`
5. **Include router** in `app/api/api.py`
6. **Write tests** in `tests/`

### Adding New Celery Task

1. **Define task** in `app/worker/tasks/`
2. **Import in** `app/worker/__init__.py`
3. **Add tests** in `tests/integration/test_celery.py`

### Adding New DICOM Processing Rule

1. **Define condition/action** in `app/services/rule_engine/`
2. **Update schemas** in `app/schemas/rules.py`
3. **Add tests** in `tests/unit/test_rule_engine.py`

---

This development setup provides a comprehensive environment for Axiom backend development with all necessary tools, configurations, and workflows.
