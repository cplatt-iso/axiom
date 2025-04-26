# Axiom Flow - Backend

Next-generation DICOM tag morphing, rule engine, and routing system designed for scalability and flexibility. This repository contains the backend API, worker processes, and database logic.

## Goals & Features

*   **DICOM Reception:**
    *   Receive DICOM objects via C-STORE (multiple configurable listeners based on `AXIOM_INSTANCE_ID`).
    *   Receive DICOM objects via DICOMweb STOW-RS API endpoint (`/api/v1/dicomweb/studies`).
*   **DICOM Polling/Querying:**
    *   Poll DICOMweb sources (QIDO-RS) for new studies/instances.
    *   Poll DIMSE sources (C-FIND) for new studies/instances.
*   **DICOM Retrieval:**
    *   Retrieve DICOM metadata/instances via DICOMweb WADO-RS (used by DICOMweb poller).
    *   Initiate DICOM retrieval via DIMSE C-MOVE (triggered by DIMSE Q/R poller).
*   **Rule Engine:**
    *   Apply complex matching criteria based on DICOM tags (equality, comparison, existence, contains, regex, list membership) and DICOM association details (Calling AE, Called AE, Source IP - *IP matching logic pending*).
    *   Match rules against specific input sources or apply globally.
    *   Support `FIRST_MATCH` or `ALL_MATCHES` execution modes per ruleset.
*   **Tag Morphing & Crosswalking:**
    *   Modify, add, or delete DICOM tags based on matched rules.
    *   Supported actions: `set`, `delete`, `prepend`, `suffix`, `regex_replace`, `copy`, `move`.
    *   **Crosswalk Action:** Perform tag value lookups and replacements based on data from external databases (MySQL, PostgreSQL, SQL Server supported via config). Uses Redis caching for lookups and optional background sync (Celery Beat) for cache pre-warming/health checks. Configured via `CrosswalkDataSource` and `CrosswalkMap` entities.
    *   Log original tag values to Original Attributes Sequence (0x0400,0x0550) when modifications occur (controlled by `LOG_ORIGINAL_ATTRIBUTES` setting - *requires full verification for all modification types*).
*   **Flexible Routing:** Send processed objects to various destinations configured as Storage Backends:
    *   Local Filesystem (within container volume mounts)
    *   Remote DICOM peers via C-STORE SCU
    *   Google Cloud Storage (GCS)
    *   Google Cloud Healthcare DICOM Store (via STOW-RS)
    *   Generic DICOMweb STOW-RS endpoints
    *   Rules link to Storage Backends via a Many-to-Many relationship.
*   **Scalability:** Designed for high throughput using asynchronous task processing (Celery/RabbitMQ) and containerization (Docker).
*   **Configuration API:** Manage all inputs (DICOMweb, DIMSE Listeners, DIMSE Q/R), outputs (Storage Backends), Crosswalk Data Sources & Mappings, Rulesets, Rules, Users, Roles, and API keys via a RESTful API (`/api/v1/docs`).
*   **Security:**
    *   User authentication via Google OAuth 2.0 (backend validates Google token, issues JWT).
    *   API Key authentication (prefix + secret, hashed storage, scoped to user).
    *   Role-Based Access Control (RBAC): Admin/User roles seeded, API endpoints protected via dependencies.
*   **Monitoring:** API endpoints provide status for core components, pollers (DICOMweb, DIMSE Q/R), listeners, and crosswalk sync jobs, including metrics (found, queued, processed counts).
*   **Database:** Uses PostgreSQL with SQLAlchemy 2.x ORM and Alembic for migrations.

## Technology Stack

*   **Backend:** Python 3.11+, FastAPI
*   **DICOM:** Pydicom, Pynetdicom
*   **Async Tasks:** Celery
*   **Message Broker:** RabbitMQ
*   **Cache/Backend:** Redis (for Celery results/backend and Crosswalk lookups)
*   **Database:** PostgreSQL
*   **ORM:** SQLAlchemy 2.x
*   **Migrations:** Alembic
*   **API Schema/Validation:** Pydantic V2
*   **Authentication:** python-jose (JWT), passlib (bcrypt), google-auth, google-api-python-client
*   **Cloud:** google-cloud-storage
*   **External DB Drivers:** psycopg[binary], mysql-connector-python, pyodbc
*   **Containerization:** Docker, Docker Compose

## Getting Started

### Prerequisites

*   Docker ([Install Docker](https://docs.docker.com/engine/install/))
*   Docker Compose ([Install Docker Compose](https://docs.docker.com/compose/install/))
*   Git

### Installation & Running

1.  **Clone the repository:**
    ```bash
    # git clone ...
    cd axiom
    ```

2.  **Configure Environment:**
    *   Copy the example environment file: `cp .env.example .env`
    *   **Edit `.env`:**
        *   Change `POSTGRES_PASSWORD`.
        *   Generate a new secure `SECRET_KEY` (`openssl rand -hex 32`).
        *   Set your `GOOGLE_OAUTH_CLIENT_ID` if using Google Login.
        *   Configure `BACKEND_CORS_ORIGINS` to include your frontend URL (e.g., `http://localhost:3000`).
        *   Set `LOG_ORIGINAL_ATTRIBUTES` to `True` or `False`.
        *   Review other DB, RabbitMQ, Redis, storage paths.
    *   *(Optional)* Place Google Cloud service account key file (e.g., `axiom-flow-gcs-key.json`) in the project root if using GCS/Healthcare backends and not relying solely on Application Default Credentials (ADC) within the container environment. Update `GOOGLE_APPLICATION_CREDENTIALS` in `docker-compose.yml` if needed.
    *   **DO NOT** commit your actual `.env` file.

3.  **Create Crosswalk Init Script (Optional for Demo):**
    *   If using the example MySQL crosswalk DB in `docker-compose.yml`, create the `crosswalk_init` directory and `crosswalk_init/init_crosswalk.sql` file as described in the setup steps. Modify the sample data as needed.

4.  **Build and Run Docker Containers:**
    ```bash
    docker compose build
    docker compose up -d
    ```
    This starts the API, Celery worker(s), Celery Beat scheduler, DIMSE listener(s), Orthanc (example peer), PostgreSQL DB, Crosswalk MySQL DB (example), message broker, and Redis.

5.  **Database Migrations:** Apply any pending database schema changes:
    ```bash
    docker compose exec api alembic upgrade head
    ```
    *(Run this initially and after pulling changes that include new migrations)*

6.  **Create Initial Superuser/Admin (Recommended):**
    *   Use the provided script (ensure DB is up):
        ```bash
        docker compose exec api python inject_admin.py
        ```
    *   *(Alternatively, first Google login with email matching `FIRST_SUPERUSER_EMAIL` might work depending on exact `crud_user.py` logic)*

7.  **Verify Services:**
    *   Check container status: `docker compose ps`
    *   View logs: `docker compose logs -f api worker beat listener crosswalk_db` (add other listeners/orthanc as needed)
    *   Access API docs: `http://localhost:8001/api/v1/docs` (default host port)
    *   Access RabbitMQ UI: `http://localhost:15672` (default user: guest, pass: guest)
    *   Access Orthanc UI: `http://localhost:8042` (default user: orthancuser, pass: orthancpassword)
    *   Connect to MySQL DB (if port exposed): Use a tool like DBeaver or `mysql` cli on host port 3306 with user `crosswalk_user` / pass `crosswalk_password_CHANGE_ME` to database `crosswalk_data`.

## Usage

1.  **Login:** Use the frontend UI (connected to this backend) with Google Login or generate an API Key via the UI/API (`/apikeys`).
2.  **Configure:** Use the frontend UI or the API endpoints (`/api/v1/docs`) to manage:
    *   Storage Backends (`/config/storage-backends`).
    *   Crosswalk Data Sources and Mappings (`/config/crosswalk/...`). Test connections and trigger syncs via API.
    *   RuleSets and Rules (`/rules-engine/*`), linking rules to destinations and adding `crosswalk` modifications referencing a configured `CrosswalkMap` ID.
    *   Input sources: DICOMweb Sources (`/config/dicomweb-sources`), DIMSE Listeners (`/config/dimse-listeners`), DIMSE Q/R Sources (`/config/dimse-qr-sources`).
    *   Users, Roles, and API keys (`/users`, `/roles`, `/apikeys`).
3.  **Send DICOM Data:**
    *   **C-STORE:** Send to the AE Title/Port defined in your active `DimseListenerConfig` records.
    *   **STOW-RS:** POST multipart/related DICOM data to `/api/v1/dicomweb/studies`.
4.  **Monitor:**
    *   Check the frontend dashboard for component status and metrics.
    *   View container logs.
    *   Use API status endpoints (`/dashboard/status`, `/system/.../status`).

## API Documentation

Interactive API documentation (Swagger UI) is available at `/api/v1/docs` when the API service is running. ReDoc documentation is at `/api/v1/redoc`.

## Current Status

*   Core architecture (API, worker, DB, message queue, cache) is functional.
*   Authentication (Google, API Key) and RBAC are implemented.
*   All planned DIMSE/DICOMweb input sources and polling/retrieval methods are implemented.
*   All planned output destinations (Filesystem, C-STORE, GCS, Google Healthcare, Generic STOW-RS) are implemented.
*   Rule engine supports tag/association matching and tag modifications (`set`, `delete`, `prepend`, `suffix`, `regex_replace`, `copy`, `move`).
*   **Crosswalk feature implemented:** Backend configuration (Data Source, Map), API, lookup service (Redis cache + live DB query), optional sync task, and `crosswalk` rule action integrated.
*   Original Attributes Sequence logging framework implemented (needs full verification).
*   Configuration via API is available for all major components.
*   Monitoring endpoints provide basic status and metrics for most components.

## Next Steps / Future Goals

*   Implement GCS Polling.
*   Implement IP Matching logic for Association Criteria.
*   Fully verify Original Attributes Sequence logging across all modification types.
*   Develop comprehensive test suite (pytest).
*   Enhance monitoring and add detailed metrics/visualizations (e.g., Grafana).
*   Implement C-GET support for DIMSE Q/R.
*   Refine error handling and user feedback.
*   UI Refinements (Crosswalk table discovery, etc.).
*   AI Integration for normalization/enrichment.
*   Deployment documentation (Kubernetes).

## Contributing

*(Placeholder: Contribution guidelines will be added here.)*

## License

*(Placeholder: MIT or Apache 2.0 recommended.)*
