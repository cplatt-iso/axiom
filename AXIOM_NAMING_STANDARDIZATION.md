# Axiom Flow Naming Standardization Summary

## Container Naming Convention: `axiom-*`

### ✅ COMPLETED - Core Infrastructure
- `dicom_processor_db` → `axiom-db`
- `dicom_processor_rabbitmq` → `axiom-rabbitmq`  
- `dicom_processor_redis` → `axiom-redis`
- `dicom_processor_elasticsearch` → `axiom-elasticsearch`
- `dicom_processor_fluent_bit` → `axiom-fluent-bit`

### ✅ COMPLETED - Application Services
- `dicom_processor_api` → `axiom-api`
- `dicom_processor_worker` → `axiom-worker`
- `dicom_processor_beat` → `axiom-beat`
- `axiom_docs` → `axiom-docs`

### ✅ COMPLETED - DICOM Services
- `dicom_processor_storescp_1` → `axiom-storescp-1`
- `dicom_processor_storescp_2` → `axiom-storescp-2`
- `dicom_processor_dcm4che_sender` → `axiom-dcm4che-sender`
- `dicom_processor_pynetdicom_sender` → `axiom-pynetdicom-sender`
- `dicom_processor_dustbin_verification` → `axiom-dustbin-verification`

## Database Naming Convention: `axiom_*`

### ✅ COMPLETED - Database Configuration
- `dicom_processor_user` → `axiom_user`
- `dicom_processor_db` → `axiom_db`
- `dicom_processor_tasks` → `axiom_flow_tasks` (Celery queue)

## Service Naming for Logging

### ✅ NEW - Standardized Service Names
Created `app/core/service_naming.py` with mapping:
- Container `axiom-api` → Service `axiom-api`
- Container `axiom-worker` → Service `axiom-worker`
- Container `axiom-storescp-1` → Service `axiom-storescp`
- Container `axiom-dcm4che-sender` → Service `axiom-dcm4che-sender`
- etc.

### ✅ UPDATED - Logging Integration
- Modified `app/core/logging_config.py` to use standardized service names
- Updated `app/services/log_service.py` to strip new `axiom-` prefix

## DICOM Configuration

### ✅ COMPLETED - DICOM AE Title
- `DICOM_PROCESSOR` → `AXIOM_FLOW`

## Files Updated:
- `core.yml` - Core infrastructure containers
- `docker/app.yml` - Application containers  
- `docker/logging.yml` - Logging infrastructure
- `docker/listeners/*.yml` - DICOM listeners
- `docker/senders/*.yml` - DICOM senders
- `docker/workers/*.yml` - Worker containers
- `docker/docs.yml` - Documentation
- `.env` - Environment variables
- `app/core/config.py` - Application configuration
- `app/worker/celery_app.py` - Celery task queue
- `app/services/log_service.py` - Log processing
- `scripts/db_dump_load.sh` - Database scripts

## Next Steps:
1. ✅ Test with container rebuild
2. 🔄 Update any remaining listener containers (mllp, dcm4che listeners)
3. 🔄 Update spanner containers if they exist
4. 🔄 Verify logging service names are working properly
5. 🔄 Update documentation references
