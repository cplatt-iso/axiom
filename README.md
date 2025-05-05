# Axiom Flow - Backend

Next-generation DICOM tag morphing, rule engine, and routing system designed for scalability and flexibility. This repository contains the backend API, worker processes, and database logic.

## Goals & Features

*   **DICOM Reception:**
    *   Receive DICOM objects via C-STORE (multiple configurable listeners based on `AXIOM_INSTANCE_ID`).
    *   Receive DICOM objects via DICOMweb STOW-RS API endpoint (`/api/v1/dicomweb/studies`).
*   **DICOM Polling/Querying:**
    *   Poll DICOMweb sources (QIDO-RS) for new studies/instances.
    *   Poll DIMSE sources (C-FIND) for new studies/instances.
    *   **Poll Google Healthcare DICOM Stores (QIDO-RS) for new studies.**
*   **DICOM Retrieval:**
    *   Retrieve DICOM metadata/instances via DICOMweb WADO-RS (used by DICOMweb poller).
    *   Initiate DICOM retrieval via DIMSE C-MOVE (triggered by DIMSE Q/R poller).
    *   **(GHC Poller currently retrieves metadata only, full instance retrieval TBD).**
*   **Rule Engine:**
    *   Apply complex matching criteria based on DICOM tags (equality, comparison, existence, contains, regex, list membership) and DICOM association details (Calling AE, Called AE, Source IP - *IP matching logic pending*).
    *   Match rules against specific input sources (DICOMweb, DIMSE Listener, DIMSE Q/R, **Google Healthcare**, STOW-RS) or apply globally.
    *   Support `FIRST_MATCH` or `ALL_MATCHES` execution modes per ruleset.
    *   **Scheduling:** Rules can be optionally linked to reusable Schedule definitions, activating them only during specified time windows (days of week, start/end times, handles overnight). Rules without a schedule are considered always active (if enabled).
*   **Tag Morphing & Crosswalking:**
    *   Modify, add, or delete DICOM tags based on matched rules.
    *   Supported actions: `set`, `delete`, `prepend`, `suffix`, `regex_replace`, `copy`, `move`, `crosswalk`.
    *   **Crosswalk Action:** Perform tag value lookups and replacements based on data from external databases (MySQL, PostgreSQL, SQL Server supported). Uses Redis caching and optional background sync. Configured via `CrosswalkDataSource` and `CrosswalkMap` entities.
    *   Log original tag values to Original Attributes Sequence (0x0400,0x0550) when modifications occur (controlled by `LOG_ORIGINAL_ATTRIBUTES` setting - *requires full verification*).
*   **Flexible Routing:** Send processed objects to various destinations configured as Storage Backends:
    *   Local Filesystem (within container volume mounts)
    *   Remote DICOM peers via C-STORE SCU (supports TLS)
    *   Google Cloud Storage (GCS)
    *   Google Cloud Healthcare DICOM Store (via STOW-RS)
    *   Generic DICOMweb STOW-RS endpoints
    *   Rules link to Storage Backends via a Many-to-Many relationship.
*   **Scalability:** Designed for high throughput using asynchronous task processing (Celery/RabbitMQ) and containerization (Docker).
*   **Configuration API:** Manage all inputs (DICOMweb, DIMSE Listeners, DIMSE Q/R, **Google Healthcare Sources**), outputs (Storage Backends), Crosswalk Data Sources & Mappings, Schedules, Rulesets, Rules, Users, Roles, and API keys via a RESTful API (`/api/v1/docs`).
*   **Security:**
    *   User authentication via Google OAuth 2.0 (backend validates Google token, issues JWT).
    *   API Key authentication (prefix + secret, hashed storage, scoped to user).
    *   Role-Based Access Control (RBAC): Admin/User roles seeded, API endpoints protected via dependencies (configurable, e.g., superuser or admin role).
    *   **TLS Support:** Implemented for outgoing DIMSE SCU operations (C-FIND, C-MOVE, C-STORE Storage Backend) and incoming DIMSE SCP (Listener). Configured via GCP Secret Manager secrets.
*   **Monitoring:** API endpoints provide status for core components, pollers (DICOMweb, DIMSE Q/R), listeners, and crosswalk sync jobs, including metrics (found, queued, processed counts).
*   **Data Browser API:** Endpoint (`/data-browser/query`) supports querying enabled DICOMweb, DIMSE Q/R, and **Google Healthcare** sources.
*   **Database:** Uses PostgreSQL with SQLAlchemy 2.x ORM and Alembic for migrations.

## Technology Stack

*   **Backend:** Python 3.11+, FastAPI
*   **DICOM:** Pydicom, Pynetdicom
*   **Async Tasks:** Celery
*   **HTTP Client:** **httpx**
*   **Async Support:** **aiohttp** (for google-auth async)
*   **Message Broker:** RabbitMQ
*   **Cache/Backend:** Redis (for Celery results/backend and Crosswalk lookups)
*   **Database:** PostgreSQL
*   **ORM:** SQLAlchemy 2.x
*   **Migrations:** Alembic
*   **API Schema/Validation:** Pydantic V2
*   **Authentication:** python-jose (JWT), passlib (bcrypt), **google-auth[aiohttp]**
*   **Cloud:** google-cloud-storage, **google-cloud-secret-manager**
*   **External DB Drivers:** psycopg[binary], mysql-connector-python, pyodbc
*   **Containerization:** Docker, Docker Compose
*   **Logging:** **structlog**

## Getting Started

### Prerequisites

*   Docker ([Install Docker](https://docs.docker.com/engine/install/))
*   Docker Compose ([Install Docker Compose](https://docs.docker.com/compose/install/))
*   Git
*   **(Optional but Recommended)** Google Cloud SDK (`gcloud`) configured for Application Default Credentials (ADC) if using GCS/GHC/Secret Manager backends/features.

### Installation & Running

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url> axiom-flow
    cd axiom-flow/backend # Adjust if backend is elsewhere
    ```

2.  **Configure Environment:**
    *   Copy the example environment file: `cp .env.example .env`
    *   **Edit `.env`:**
        *   Change `POSTGRES_PASSWORD`.
        *   Generate a new secure `SECRET_KEY`.
        *   Set your `GOOGLE_OAUTH_CLIENT_ID`.
        *   Configure `BACKEND_CORS_ORIGINS` (e.g., `https://your-frontend-domain.com`).
        *   Review DB, RabbitMQ, Redis settings.
        *   Review storage paths (`DICOM_STORAGE_PATH`, etc.) and ensure corresponding volumes are mapped in `docker-compose.yml`.
    *   **(Optional/Required for GCS/GHC/Secrets):** Configure Google Cloud Authentication.
        *   **ADC (Recommended):** Run `gcloud auth application-default login` on your host machine *before* starting containers if volumes mount your ADC file, OR ensure the service account running the container has necessary IAM permissions.
        *   **Service Account Key:** Place the key file (e.g., `axiom-flow-gcs-key.json`) accessible to the containers and update `GOOGLE_APPLICATION_CREDENTIALS` environment variable in `docker-compose.yml`.
    *   **TLS Secrets:** If using TLS for DIMSE, create the necessary certificates/keys and upload them to GCP Secret Manager. Update references in configuration (e.g., `tls_ca_cert_secret_name`) with the full Secret Manager resource name (e.g., `projects/.../secrets/.../versions/latest`).

3.  **Build and Run Docker Containers:** (Run from the directory containing `docker-compose.yml`)
    ```bash
    docker compose build
    docker compose up -d
    ```

4.  **Database Migrations:** Apply any pending database schema changes:
    ```bash
    docker compose exec api alembic upgrade head
    ```
    *(Run initially and after pulling changes with new migrations)*

5.  **Create Initial Superuser/Admin:**
    ```bash
    docker compose exec api python inject_admin.py
    ```
    *(Verify in DB if `is_superuser=True` is needed for full access based on API dependencies)*

6.  **Verify Services:**
    *   Check container status: `docker compose ps`
    *   View logs: `docker compose logs -f api worker beat` (and others as needed)
    *   Access API docs: `http://localhost:8001/api/v1/docs` (or your mapped host port)

## Usage

1.  **Login:** Use the frontend UI with Google Login or an API Key.
2.  **Configure:** Use the UI or API to manage:
    *   Storage Backends
    *   Schedules
    *   Crosswalk Data Sources & Mappings
    *   RuleSets & Rules (linking to destinations, optional schedules, modifications)
    *   Input sources: DICOMweb, DIMSE Listeners, DIMSE Q/R, **Google Healthcare Sources**.
    *   Users, Roles, API keys.
3.  **Send/Poll Data:** Configure sources and destinations, enable rules. Data received via Listener/STOW or polled via DICOMweb/DIMSE Q/R/**GHC Poller** will be processed.
4.  **Monitor:** Use dashboard, logs, API status endpoints.

## API Documentation

Interactive API documentation (Swagger UI) is available at `/api/v1/docs`. ReDoc documentation is at `/api/v1/redoc`.

## Current Status

*   Core architecture functional.
*   Authentication (Google, API Key) and RBAC implemented.
*   All planned input sources (C-STORE, STOW-RS, DICOMweb Poll, DIMSE Q/R Poll, **GHC Poll**) implemented.
*   All planned output destinations implemented (Filesystem, C-STORE, GCS, GHC STOW, DICOMweb STOW). TLS supported for C-STORE SCU.
*   Rule engine supports tag/association matching (IP Ops pending), **scheduling**, and tag modifications (`set`, `delete`, `prepend`, `suffix`, `regex_replace`, `copy`, `move`, `crosswalk`).
*   Crosswalk feature fully implemented.
*   Scheduling feature fully implemented.
*   **Google Healthcare polling and basic metadata processing task implemented.**
*   **Data Browser API supports querying DICOMweb, DIMSE Q/R, and Google Healthcare sources.**
*   Configuration via API available for all major components.
*   Monitoring endpoints functional.
*   Original Attributes Sequence logging framework in place (needs verification).
*   Secrets management via GCP Secret Manager integrated for TLS.

## Next Steps / Future Goals

*   **Implement IP Matching (Backend):** Logic for association criteria.
*   **Verify Original Attributes Logging (Backend):** Test across all modification types.
*   **Enhance GHC Poller Processing:** Implement full study retrieval (WADO?) or instance-level processing based on polled metadata.
*   **Seed/Dump Script Overhaul:** Make config seeding/dumping robust.
*   **Testing:** Develop comprehensive backend (pytest) and frontend test suites.
*   **UI Refinements:** Rule testing feature, dashboard visuals.
*   **Logging Improvements:** Route all logs via Fluentd, fix Uvicorn/Postgres text logs.
*   **Documentation:** Add API examples, deployment guides.
*   **Longer-Term:** C-GET support, AI integration (Gemini?), Kubernetes.

## Contributing

*(Placeholder)*

## License

*(Placeholder)*
