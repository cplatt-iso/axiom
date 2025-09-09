# Axiom Unified JSON Logging Configuration

## Overview

This document outlines the standardized JSON logging configuration implemented across all Axiom containers to ensure consistent log format for ELK stack ingestion.

## Changes Made

### 1. Centralized Logging Configuration (`app/core/logging_config.py`)

Created a unified logging configuration module that provides:
- `configure_json_logging()` - General JSON logging for all services
- `configure_celery_logging()` - Specialized configuration for Celery workers
- `configure_uvicorn_logging()` - Configuration for FastAPI/Uvicorn applications

### 2. Container-Specific Updates

#### ✅ FastAPI API Container (`dicom_processor_api`)
- **File**: `app/main.py`
- **Status**: Updated to use centralized logging
- **Output**: JSON structured logs

#### ✅ Celery Worker Container (`dicom_processor_worker`)
- **File**: `app/worker/celery_app.py`
- **Status**: Updated to use centralized Celery logging
- **Output**: JSON structured logs
- **Note**: Handles Celery-specific loggers properly

#### ✅ Celery Beat Container (`dicom_processor_beat`)
- **Status**: Uses same configuration as worker
- **Output**: JSON structured logs

#### ✅ DIMSE Listeners (`dicom_processor_storescp_1`, etc.)
- **File**: `app/services/network/dimse/server.py`
- **Status**: Updated to use centralized logging
- **Output**: JSON structured logs

#### ✅ Dustbin Verification Worker
- **File**: `app/worker/dustbin_verification_worker.py`
- **Status**: Updated to use centralized logging for standalone execution
- **Output**: JSON structured logs

#### ⚠️ Senders (DCM4CHE, PyNetDicom, etc.)
- **File**: `app/senders/dcm4che_sender.py` (partially updated)
- **Status**: Logging configuration added, but print statements need conversion
- **TODO**: Replace remaining `print()` statements with structured logging

#### 📋 PostgreSQL (`dicom_processor_db`)
- **Status**: Enhanced logging configuration with structured fields
- **Output**: Structured text logs (native Postgres format with enhanced fields)
- **Note**: Sent to fluentd for ELK processing

#### ✅ RabbitMQ & Redis
- **Status**: Native log formats sent to fluentd
- **Output**: Container logs processed by fluentd

## Log Format

All application logs now follow this JSON structure:

```json
{
  "event": "Log message content",
  "level": "info|debug|warning|error",
  "logger": "module.name",
  "timestamp": "2025-08-23T18:51:44.832273Z",
  "service": "service_name",
  "additional_field": "contextual_data"
}
```

## Environment Variables

The following environment variable controls logging level across all services:

```bash
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
```

## Validation

Use the provided validation script:

```bash
./validate_logging.sh
```

This script will:
1. Check all running containers
2. Sample recent logs from each container
3. Validate JSON format compliance
4. Report any non-compliant logs

## Docker Compose Configuration

All containers use fluentd logging driver:

```yaml
logging:
  driver: "fluentd"
  options:
    fluentd-address: "127.0.0.1:24224"
    tag: "axiom.service_name.{{.Name}}"
    mode: non-blocking
    max-buffer-size: "10m"
    fluentd-retry-wait: "1s"
    fluentd-max-retries: "60"
```

## Remaining Tasks

### High Priority
1. **Finish Sender Logging Updates**: Replace all `print()` statements in sender modules with structured logging:
   - `app/senders/dcm4che_sender.py`
   - `app/senders/pynetdicom_sender.py` 
   - Other sender modules

### Medium Priority  
2. **Test Logging Configuration**: Run full system test to validate all containers output JSON
3. **Update Any Missing Modules**: Scan for any other Python modules that might need logging updates

### Low Priority
4. **Enhanced PostgreSQL Logging**: Consider custom PostgreSQL image with JSON logging capability
5. **Log Parsing Rules**: Optimize fluentd/ELK parsing rules for the new format

## Testing the Changes

1. **Rebuild containers**:
   ```bash
   ./axiomctl down
   ./axiomctl build
   ./axiomctl up -d
   ```

2. **Validate logging**:
   ```bash
   ./validate_logging.sh
   ```

3. **Monitor logs**:
   ```bash
   ./axiomctl logs -f worker
   ./axiomctl logs -f api
   ```

## Expected Output

After the changes, your container logs should look like this:

```json
{"event": "Task started: retry_pending_exceptions_task", "level": "info", "logger": "app.worker.tasks.exception_management", "timestamp": "2025-08-23T18:52:04.000000Z", "task_id": "4243d6b5-c163-4c05-a2fe-72004c24f58d", "task_name": "retry_pending_exceptions_task"}

{"event": "Updating heartbeat only...", "level": "debug", "logger": "dicom_listener", "timestamp": "2025-08-23T18:51:49.529032Z", "listener_instance_id": "storescp_1", "config_name": "DIMSE C-STORE SCP", "config_id": 1, "ae_title": "AXIOM_SCP", "port": 11112, "tls_enabled": false, "listen_address": "0.0.0.0:11112"}
```

## Support

If you encounter any issues with the logging configuration:

1. Check the validation script output
2. Verify that `structlog` is installed in all containers
3. Ensure `LOG_LEVEL` environment variable is set properly
4. Review container logs for any import errors related to the logging configuration

The centralized logging configuration ensures consistency across all services and makes it easy to modify logging behavior from a single location.
