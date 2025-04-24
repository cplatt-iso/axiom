# Axiom Flow - Backend

Next-generation DICOM tag morphing, rule engine, and routing system designed for scalability and flexibility. This repository contains the backend API, worker processes, and database logic.

## Goals & Features

*   **DICOM Reception:**
    *   Receive DICOM objects via C-STORE (multiple configurable listeners).
    *   Receive DICOM objects via DICOMweb STOW-RS endpoint.
*   **DICOM Polling/Querying:**
    *   Poll DICOMweb sources (QIDO-RS) for new studies/instances.
    *   Poll DIMSE sources (C-FIND) for new studies.
*   **DICOM Retrieval:**
    *   Retrieve DICOM metadata/instances via DICOMweb WADO-RS (used by DICOMweb poller).
    *   Initiate DICOM retrieval via DIMSE C-MOVE (used by DIMSE Q/R poller).
*   **Rule Engine:** Apply complex matching criteria based on DICOM tags and input source identifiers.
*   **Tag Morphing:** Modify, add, or delete DICOM tags based on matched rules (including regex replace, prepend, suffix).
*   **Flexible Routing:** Send processed objects to various destinations:
    *   Local Filesystem
    *   Remote DICOM peers via C-STORE
    *   Google Cloud Storage (GCS)
    *   Google Cloud Healthcare DICOM Store (via STOW-RS)
    *   Generic DICOMweb STOW-RS endpoints
*   **Scalability:** Designed for high throughput using asynchronous task processing (Celery/RabbitMQ) and containerization (Docker).
*   **Configuration:** Manage all inputs, outputs, rulesets, and rules via a RESTful API and a Web UI (see frontend repository).
*   **Security:** User authentication (Google OAuth, API Keys) and Role-Based Access Control (RBAC - Admin/User roles).
*   **Monitoring:** API endpoints provide status for core components, pollers, and listeners, including basic metrics (found, queued, processed counts).
*   **Database:** Uses PostgreSQL with SQLAlchemy ORM and Alembic for migrations.

## Technology Stack

*   **Backend:** Python 3.11+, FastAPI
*   **DICOM:** Pydicom, Pynetdicom
*   **Async Tasks:** Celery
*   **Message Broker:** RabbitMQ
*   **Cache/Backend:** Redis (for Celery results/backend)
*   **Database:** PostgreSQL
*   **ORM:** SQLAlchemy 2.x
*   **Migrations:** Alembic
*   **API Schema/Validation:** Pydantic V2
*   **Authentication:** python-jose (JWT), passlib (hashing), google-auth
*   **Cloud:** google-cloud-storage
*   **Containerization:** Docker, Docker Compose

## Getting Started

### Prerequisites

*   Docker ([Install Docker](https://docs.docker.com/engine/install/))
*   Docker Compose ([Install Docker Compose](https://docs.docker.com/compose/install/))
*   Git

### Installation & Running

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/[YourUsername]/axiom_backend.git # Or your repo name
    cd axiom_backend
    ```

2.  **Configure Environment:**
    *   Copy the example environment file:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file. **Crucially:**
        *   Change `POSTGRES_PASSWORD`.
        *   Generate a new secure `SECRET_KEY` (e.g., `openssl rand -hex 32`).
        *   Set your `GOOGLE_OAUTH_CLIENT_ID` if using Google Login.
        *   Configure `BACKEND_CORS_ORIGINS` to include your frontend URL (e.g., `http://localhost:3000`, `https://your-frontend.com`).
        *   Adjust database, RabbitMQ, Redis connection details if not using defaults or Docker Compose service names.
        *   Review storage paths (`DICOM_STORAGE_PATH`, `DICOM_ERROR_PATH`, `FILESYSTEM_STORAGE_PATH`).
    *   **DO NOT** commit your actual `.env` file.

3.  **Build and Run Docker Containers:**
    ```bash
    docker compose build
    docker compose up -d
    ```
    This starts the API, Celery worker(s), Celery Beat scheduler, DICOM listener(s), database, message broker, and Redis.

4.  **Database Migrations:** Apply any pending database schema changes:
    ```bash
    docker compose exec api alembic upgrade head
    ```
    *(Run this initially and after pulling changes that include new migrations)*

5.  **Create Initial Superuser/Admin (Optional but Recommended):**
    *   If the database is empty, the first user logging in via Google with the email matching `FIRST_SUPERUSER_EMAIL` in `.env` might be granted superuser status (check `crud_user.py` logic if implemented).
    *   Alternatively, use the provided script (ensure DB is accessible):
        ```bash
        # Make sure DATABASE_URI in inject_admin.py points correctly
        # or set ADMIN_SCRIPT_DB_URI environment variable
        docker compose exec api python inject_admin.py
        ```

6.  **Verify Services:**
    *   Check container status: `docker compose ps`
    *   View logs: `docker compose logs -f api worker beat listener` (add other listener names if applicable)
    *   Access API docs: `http://localhost:8001/api/v1/docs` (or the host port mapped in `docker-compose.yml`)
    *   Access RabbitMQ UI: `http://localhost:15672` (default user: guest, pass: guest - or as set in `.env`)

## Usage

1.  **Configure:** Use the frontend UI or the API endpoints (documented at `/api/v1/docs`) to:
    *   Create Storage Backend configurations (`/config/storage-backends`).
    *   Create RuleSets and Rules (`/rules-engine/rulesets`, `/rules-engine/rules`), linking rules to desired Storage Backend destinations by ID.
    *   Configure input sources: DICOMweb Sources (`/config/dicomweb-sources`), DIMSE Listeners (`/config/dimse-listeners`), DIMSE Q/R Sources (`/config/dimse-qr-sources`).
    *   Manage users, roles, and API keys (`/users`, `/roles`, `/apikeys`).
2.  **Send DICOM Data:**
    *   **C-STORE:** Send to the AE Title/Port defined in your DIMSE Listener configurations. Ensure the listener container with the matching `AXIOM_INSTANCE_ID` is running.
    *   **STOW-RS:** POST multipart/related DICOM data to `/api/v1/dicomweb/studies`. Requires authentication (Bearer token or API Key).
3.  **Monitor:**
    *   Check the frontend dashboard.
    *   View container logs.
    *   Use API status endpoints (`/dashboard/status`, `/system/.../status`).

## Testing

*(Placeholder: Test suite setup using pytest will be added here.)*

## Contributing

*(Placeholder: Contribution guidelines will be added here.)*

## License

*(Placeholder: Choose and add a license, e.g., MIT or Apache 2.0.)*
