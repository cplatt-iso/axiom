# System Configuration API

This document describes the new System Configuration API that allows runtime modification of system settings without editing configuration files.

## Overview

The System Configuration API provides a secure, admin-protected interface for managing system settings at runtime. This eliminates the need for users to edit configuration files in production environments while maintaining proper validation and persistence.

## Security

All endpoints require the `Admin` role for access. The API uses the existing role-based access control system.

## Configuration Categories

Settings are organized into logical categories:

### Processing (`processing`)
- `DELETE_ON_SUCCESS`: Delete original files after successful processing
- `DELETE_UNMATCHED_FILES`: Delete files that don't match any routing rules  
- `DELETE_ON_NO_DESTINATION`: Delete files when no destination is configured
- `MOVE_TO_ERROR_ON_PARTIAL_FAILURE`: Move files to error directory on partial processing failure
- `LOG_ORIGINAL_ATTRIBUTES`: Log original DICOM attributes for debugging

### Dustbin System (`dustbin`)
- `USE_DUSTBIN_SYSTEM`: Enable medical-grade dustbin file safety system
- `DUSTBIN_RETENTION_DAYS`: Days to retain files in dustbin (1-365)
- `DUSTBIN_VERIFICATION_TIMEOUT_HOURS`: Hours to wait for destination confirmations (1-168)

### Batch Processing (`batch_processing`)
- `EXAM_BATCH_COMPLETION_TIMEOUT`: Seconds to wait before considering a study complete (1-60)
- `EXAM_BATCH_CHECK_INTERVAL`: Seconds between completion checks (1-30)
- `EXAM_BATCH_MAX_CONCURRENT`: Maximum concurrent batches to process (1-50)

### Cleanup (`cleanup`)
- `STALE_DATA_CLEANUP_AGE_DAYS`: Age in days for cleaning up stale data (1-365)
- `CLEANUP_BATCH_SIZE`: Records to process per cleanup batch (10-1000)

### Celery (`celery`)
- `CELERY_WORKER_CONCURRENCY`: Number of concurrent Celery workers (1-32)
- `CELERY_PREFETCH_MULTIPLIER`: Task prefetch multiplier for workers (1-16)
- `CELERY_TASK_MAX_RETRIES`: Maximum task retry attempts (0-10)

### DICOMweb (`dicomweb`)
- `DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS`: Default fallback days for DICOMweb polling (1-30)
- `DICOMWEB_POLLER_QIDO_LIMIT`: QIDO-RS query result limit (100-10000)
- `DICOMWEB_POLLER_MAX_SOURCES`: Maximum number of DICOMweb sources (1-500)

### AI (`ai`)
- `AI_VOCAB_CACHE_ENABLED`: Enable AI vocabulary caching
- `AI_VOCAB_CACHE_TTL_SECONDS`: AI vocabulary cache TTL in seconds (300-31536000)
- `VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB`: Maximum output tokens for Vertex AI vocab requests (50-1000)

## API Endpoints

### GET /api/v1/system-config/categories
List all available configuration categories.

**Response:** Array of category names

### GET /api/v1/system-config/
Get all system configuration settings or filter by category.

**Query Parameters:**
- `category` (optional): Filter by specific category

**Response:** Array of configuration objects with:
- `key`: Setting key name
- `category`: Category name  
- `value`: Current effective value
- `type`: Data type (`boolean`, `integer`, `string`)
- `description`: Human-readable description
- `default`: Default value
- `min_value`/`max_value`: Validation constraints (for integers)
- `is_modified`: Whether the setting has been changed from default

### PUT /api/v1/system-config/{setting_key}
Update a specific configuration setting.

**Request Body:**
```json
{
  "value": <new_value>
}
```

**Response:** Updated configuration object

**Errors:**
- `404`: Setting not found or not modifiable
- `400`: Invalid value for setting

### POST /api/v1/system-config/bulk-update
Update multiple configuration settings at once.

**Request Body:**
```json
{
  "settings": {
    "SETTING_KEY1": value1,
    "SETTING_KEY2": value2
  },
  "ignore_errors": false
}
```

**Response:** Array of updated configuration objects

### DELETE /api/v1/system-config/{setting_key}
Reset a configuration setting to its default value.

**Response:** `204 No Content`

### POST /api/v1/system-config/reload
Trigger configuration reload (where applicable).

**Response:**
```json
{
  "status": "success",
  "message": "Configuration reload initiated. Some changes may require service restart."
}
```

## How It Works

1. **Default Values**: Settings start with their default values from the `settings` object
2. **Database Overrides**: Admin changes are stored in the `system_settings` table
3. **Effective Values**: The API returns the database override if present, otherwise the default
4. **Type Safety**: All values are validated according to their expected type and constraints
5. **Persistence**: Changes survive application restarts

## Usage Examples

### Get all processing-related settings
```bash
curl -H 'Authorization: Bearer TOKEN' \
     'http://localhost:8000/api/v1/system-config/?category=processing'
```

### Update dustbin retention period
```bash
curl -X PUT \
     -H 'Authorization: Bearer TOKEN' \
     -H 'Content-Type: application/json' \
     -d '{"value": 45}' \
     'http://localhost:8000/api/v1/system-config/DUSTBIN_RETENTION_DAYS'
```

### Bulk update multiple settings
```bash
curl -X POST \
     -H 'Authorization: Bearer TOKEN' \
     -H 'Content-Type: application/json' \
     -d '{
       "settings": {
         "DELETE_ON_SUCCESS": false,
         "CELERY_WORKER_CONCURRENCY": 12,
         "AI_VOCAB_CACHE_ENABLED": true
       }
     }' \
     'http://localhost:8000/api/v1/system-config/bulk-update'
```

### Reset setting to default
```bash
curl -X DELETE \
     -H 'Authorization: Bearer TOKEN' \
     'http://localhost:8000/api/v1/system-config/DELETE_ON_SUCCESS'
```

## Integration with Existing Code

To use dynamic configuration values in your code:

```python
from app.services.config_service import config_service
from app.api import deps

# In an endpoint or service
def some_function(db: Session = Depends(deps.get_db)):
    # Get effective setting value
    delete_on_success = config_service.get_effective_setting(
        db, "DELETE_ON_SUCCESS", default_value=False
    )
    
    # Use the value
    if delete_on_success:
        # Delete the file
        pass
```

## Frontend Integration

The frontend can now provide a configuration management interface that:
- Lists all configurable settings by category
- Shows current values vs defaults
- Provides form validation based on type and constraints  
- Allows bulk configuration changes
- Shows which settings have been modified
- Provides reset functionality

This eliminates the need for users to edit configuration files in production while maintaining security and data integrity.
