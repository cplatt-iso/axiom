# Axiom Flow

Next-generation DICOM tag morphing, rule engine, and routing system designed for scalability and flexibility, with planned AI integration.

## Goals & Features

*   **DICOM Reception:** Receive DICOM objects via C-STORE. (Future: DICOMweb STOW-RS).
*   **Rule Engine:** Apply complex matching criteria based on DICOM tags.
*   **Tag Morphing:** Modify/add/delete DICOM tags based on matched rules.
*   **Flexible Routing:** Send processed objects to various destinations (Filesystem, C-STORE, API PUT, Cloud Storage).
*   **Scalability:** Designed to handle millions of exams per month using asynchronous task processing (Celery/RabbitMQ) and container orchestration (Docker, Kubernetes planned).
*   **Configuration:** Rule management via API and Web UI.
*   **Deployment:** Deployable on private or public cloud infrastructure.
*   **Security:** User authentication and Role-Based Access Control (RBAC).
*   **Extensibility:** Support for multiple database backends and storage targets.
*   **AI Assistance (Future):** Integrate AI for rule creation assistance, system monitoring, and potentially advanced processing.
*   **Modern UI:** Simple, functional web interface with theming (light/dark).

## Technology Stack

*   **Backend:** Python 3.11+, FastAPI
*   **DICOM:** Pydicom, Pynetdicom
*   **Async Tasks:** Celery
*   **Message Broker:** RabbitMQ
*   **Database:** PostgreSQL (via SQLAlchemy for multi-backend support)
*   **ORM:** SQLAlchemy 2.x
*   **API Schema/Validation:** Pydantic V2
*   **Frontend (Planned):** React, Tailwind CSS
*   **Containerization:** Docker, Docker Compose (Kubernetes planned)
*   **Authentication (Planned):** OAuth2/OIDC integration

## Getting Started

### Prerequisites

*   Docker ([Install Docker](https://docs.docker.com/engine/install/))
*   Docker Compose ([Install Docker Compose](https://docs.docker.com/compose/install/))
*   Git

### Installation & Running

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/[YourUsername]/[YourChosenName].git
    cd [YourChosenName]
    ```

2.  **Configure Environment:**
    *   Copy the example environment file:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file. Pay attention to:
        *   `POSTGRES_PASSWORD`: Change the default password.
        *   `SECRET_KEY`: **Generate a new secure secret key.** (e.g., using `openssl rand -hex 32`)
        *   Other settings as needed (DICOM port, storage paths if not using defaults).
        *   **DO NOT** commit your actual `.env` file to version control.

3.  **Build and Run Docker Containers:**
    ```bash
    docker compose build
    docker compose up -d
    ```
    This will build the images and start the following services:
    *   `db`: PostgreSQL database
    *   `rabbitmq`: RabbitMQ message broker
    *   `api`: FastAPI web server / API
    *   `worker`: Celery worker for processing tasks
    *   `listener`: DICOM C-STORE SCP listener

4.  **Verify Services:**
    *   Check container status: `docker compose ps`
    *   View logs: `docker compose logs -f api worker listener`
    *   Access API docs: `http://localhost:8001/docs` (or the host port mapped in `docker-compose.yml`)
    *   Access RabbitMQ UI: `http://localhost:15672` (user: guest, pass: guest - or as set in `.env`)

## Usage

1.  **Manage Rules:** Use the API endpoints documented at `/docs` (under "Rules Engine") to create/read/update/delete RuleSets and Rules.
2.  **Send DICOM Data:**
    *   Use a DICOM toolkit (e.g., `dcmtk`) to send studies via C-STORE to the listener service:
        ```bash
        # Example using storescu
        storescu <your_host_ip> <DICOM_SCP_PORT> /path/to/dicom_file.dcm -aec <DICOM_SCP_AE_TITLE> -aet <Your_SCU_AE_Title>
        ```
        *(Replace values with those from your `.env` file and your setup)*
    *   Check the `listener` and `worker` logs to monitor processing.

## Testing

*(Placeholder: Test suite setup using pytest will be added here.)*

## Contributing

*(Placeholder: Contribution guidelines will be added here.)*

## License

*(Placeholder: Choose and add a license, e.g., MIT or Apache 2.0.)*
