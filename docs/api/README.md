# API Reference

Axiom provides a comprehensive RESTful API for configuration, monitoring, and data operations. This reference covers all endpoints with examples and best practices.

## Base URL and Versioning

**Base URL:** `https://axiom.yourdomain.com/api/v1`  
**API Version:** v1 (current)  
**Content Type:** `application/json` (unless specified otherwise)

## Authentication

Axiom supports two authentication methods:

### 1. OAuth 2.0 (Interactive)
For web applications and interactive use.

```bash
# Get OAuth token from Google
POST /api/v1/auth/google
Content-Type: application/json

{
  "token": "google_oauth_token_here"
}

# Response
{
  "access_token": "jwt_token_here",
  "token_type": "bearer",
  "expires_in": 3600,
  "user": {
    "id": "user_id",
    "email": "user@example.com",
    "role": "admin"
  }
}
```

### 2. API Keys (Service-to-Service)
For programmatic access and integrations.

```bash
# Create API key (requires OAuth token)
POST /api/v1/auth/api-keys
Authorization: Bearer jwt_token_here
Content-Type: application/json

{
  "name": "integration-service",
  "role": "user",
  "expires_in_days": 365
}

# Response
{
  "id": "key_id",
  "name": "integration-service", 
  "prefix": "ak_prod",
  "secret": "full_api_key_here",
  "role": "user",
  "created_at": "2025-09-08T10:30:00Z",
  "expires_at": "2026-09-08T10:30:00Z"
}

# Use API key
GET /api/v1/config/storage-backends
Authorization: Api-Key ak_prod_1234567890abcdef...
```

## Configuration Management

### Storage Backends

Storage backends define where processed DICOM files are sent.

#### List Storage Backends
```bash
GET /api/v1/config/storage-backends
Authorization: Api-Key your_api_key

# Response
{
  "items": [
    {
      "id": "backend_id",
      "name": "primary-pacs",
      "type": "dicom_scu",
      "config": {
        "ae_title": "DEST_PACS",
        "host": "pacs.hospital.com",
        "port": 104,
        "timeout": 30
      },
      "enabled": true,
      "created_at": "2025-09-08T10:30:00Z"
    }
  ],
  "total": 1,
  "page": 1,
  "per_page": 50
}
```

#### Create Storage Backend
```bash
POST /api/v1/config/storage-backends
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "name": "cloud-storage",
  "type": "gcs",
  "config": {
    "bucket_name": "axiom-dicom-prod",
    "prefix": "studies/",
    "credentials_path": "/app/secrets/gcp-service-account.json"
  },
  "enabled": true,
  "description": "Primary cloud storage for processed studies"
}
```

#### Storage Backend Types

**File System:**
```json
{
  "type": "filesystem",
  "config": {
    "base_path": "/data/dicom",
    "create_directories": true,
    "permissions": "0755"
  }
}
```

**DICOM SCU (Send to PACS):**
```json
{
  "type": "dicom_scu", 
  "config": {
    "ae_title": "DEST_PACS",
    "host": "pacs.hospital.com",
    "port": 104,
    "timeout": 30,
    "tls_enabled": false
  }
}
```

**Google Cloud Storage:**
```json
{
  "type": "gcs",
  "config": {
    "bucket_name": "axiom-dicom",
    "prefix": "studies/",
    "credentials_path": "/app/secrets/gcp-service-account.json"
  }
}
```

**Google Healthcare API:**
```json
{
  "type": "google_healthcare",
  "config": {
    "project_id": "healthcare-project",
    "location": "us-central1", 
    "dataset_id": "imaging-dataset",
    "dicom_store_id": "main-store"
  }
}
```

**DICOMweb STOW-RS:**
```json
{
  "type": "dicomweb_stow",
  "config": {
    "base_url": "https://pacs.hospital.com/dicomweb",
    "auth_type": "bearer_token",
    "credentials": {"token": "bearer_token_here"}
  }
}
```

### Rule Engine

#### List Rulesets
```bash
GET /api/v1/config/rulesets
Authorization: Api-Key your_api_key

# Query parameters
?enabled=true&source_type=c_store&page=1&per_page=50
```

#### Create Ruleset
```bash
POST /api/v1/config/rulesets
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "name": "ct-routing",
  "description": "Route CT studies based on body part",
  "enabled": true,
  "execution_mode": "FIRST_MATCH",
  "source_types": ["c_store", "stow_rs"],
  "schedule_id": null,
  "rules": [
    {
      "name": "chest-ct-rule",
      "enabled": true,
      "description": "Route chest CT studies to specialized storage",
      "conditions": [
        {
          "type": "tag_equals",
          "tag": "(0x0008,0x0060)",
          "value": "CT"
        },
        {
          "type": "tag_contains",
          "tag": "(0x0018,0x0015)",
          "value": "CHEST"
        }
      ],
      "actions": [
        {
          "type": "set",
          "tag": "(0x0008,0x103E)",
          "value": "CHEST CT - PROCESSED"
        }
      ],
      "storage_backends": ["chest-ct-storage"]
    }
  ]
}
```

#### Rule Condition Types

**Tag Equality:**
```json
{
  "type": "tag_equals",
  "tag": "(0x0008,0x0060)",
  "value": "CT"
}
```

**Tag Existence:**
```json
{
  "type": "tag_exists", 
  "tag": "(0x0010,0x0030)"
}
```

**Tag Contains:**
```json
{
  "type": "tag_contains",
  "tag": "(0x0010,0x0010)",
  "value": "JOHN",
  "case_sensitive": false
}
```

**Numeric Comparison:**
```json
{
  "type": "tag_numeric",
  "tag": "(0x0018,0x0050)",
  "operator": "greater_than",
  "value": 5.0
}
```

**Regular Expression:**
```json
{
  "type": "tag_regex",
  "tag": "(0x0010,0x0020)",
  "pattern": "^PAT\\d{6}$"
}
```

**List Membership:**
```json
{
  "type": "tag_in_list",
  "tag": "(0x0008,0x0060)",
  "values": ["CT", "MR", "US"]
}
```

**Association Criteria:**
```json
{
  "type": "association_ae",
  "calling_ae": "MODALITY_AE",
  "called_ae": "AXIOM"
}
```

#### Rule Action Types

**Set Tag Value:**
```json
{
  "type": "set",
  "tag": "(0x0008,0x103E)",
  "value": "PROCESSED STUDY"
}
```

**Delete Tag:**
```json
{
  "type": "delete",
  "tag": "(0x0010,0x4000)"
}
```

**Copy Tag:**
```json
{
  "type": "copy",
  "source_tag": "(0x0010,0x0020)",
  "target_tag": "(0x0010,0x1000)"
}
```

**Crosswalk Lookup:**
```json
{
  "type": "crosswalk",
  "tag": "(0x0018,0x0015)",
  "crosswalk_map_id": "body_part_standardization"
}
```

**AI Standardization:**
```json
{
  "type": "ai_standardize",
  "tag": "(0x0018,0x0015)",
  "ai_config_id": "body_part_gemini"
}
```

### Data Sources

#### DICOM Listeners (C-STORE SCP)
```bash
POST /api/v1/config/dimse-listeners
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "name": "main-listener",
  "ae_title": "AXIOM",
  "port": 11112,
  "host": "0.0.0.0",
  "max_pdu_size": 16384,
  "enabled": true,
  "instance_filter": "prod"
}
```

#### DICOMweb Sources (Polling)
```bash
POST /api/v1/config/dicomweb-sources
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "name": "hospital-pacs",
  "base_url": "https://pacs.hospital.com/dicomweb",
  "auth_type": "bearer_token", 
  "credentials": {"token": "bearer_token_here"},
  "poll_interval": 300,
  "enabled": true,
  "query_params": {
    "StudyDate": "20250908-",
    "Modality": "CT,MR"
  }
}
```

#### DIMSE Query/Retrieve Sources
```bash
POST /api/v1/config/dimse-qr-sources
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "name": "legacy-pacs",
  "ae_title": "LEGACY_PACS",
  "host": "pacs.legacy.com", 
  "port": 104,
  "move_destination": "AXIOM",
  "query_level": "STUDY",
  "poll_interval": 600,
  "enabled": true
}
```

## Monitoring and Operations

### Health Checks

#### System Health
```bash
GET /health
# No authentication required

# Response
{
  "status": "healthy",
  "timestamp": "2025-09-08T10:30:00Z",
  "version": "2.0.1",
  "uptime_seconds": 86400
}
```

#### Detailed Health Check
```bash
GET /api/v1/monitoring/health
Authorization: Api-Key your_api_key

# Response
{
  "status": "healthy",
  "components": {
    "database": {"status": "healthy", "response_time_ms": 5},
    "redis": {"status": "healthy", "response_time_ms": 2},
    "rabbitmq": {"status": "healthy", "response_time_ms": 3},
    "storage": {"status": "healthy", "available_space_gb": 1024},
    "workers": {"status": "healthy", "active_workers": 4}
  },
  "timestamp": "2025-09-08T10:30:00Z"
}
```

### Metrics and Statistics

#### Processing Statistics
```bash
GET /api/v1/monitoring/stats
Authorization: Api-Key your_api_key

# Query parameters
?start_date=2025-09-01&end_date=2025-09-08&granularity=daily

# Response
{
  "period": {
    "start": "2025-09-01T00:00:00Z",
    "end": "2025-09-08T23:59:59Z"
  },
  "metrics": {
    "studies_processed": 15420,
    "studies_failed": 23,
    "processing_time_avg_seconds": 2.3,
    "storage_backends_used": {
      "primary-pacs": 12000,
      "cloud-storage": 15420,
      "archive-storage": 3420
    },
    "rules_executed": 45670,
    "ai_standardizations": 8900
  },
  "daily_breakdown": [
    {
      "date": "2025-09-01",
      "studies_processed": 2150,
      "studies_failed": 3
    }
  ]
}
```

#### Queue Status
```bash
GET /api/v1/monitoring/queue-status
Authorization: Api-Key your_api_key

# Response
{
  "queues": {
    "celery": {
      "pending": 156,
      "active": 8,
      "failed": 2
    },
    "high_priority": {
      "pending": 12,
      "active": 4,
      "failed": 0
    }
  },
  "workers": {
    "total": 4,
    "active": 4,
    "offline": 0
  },
  "timestamp": "2025-09-08T10:30:00Z"
}
```

### Exception Management

#### List Exceptions
```bash
GET /api/v1/monitoring/exceptions
Authorization: Api-Key your_api_key

# Query parameters
?status=pending&error_type=storage_error&page=1&per_page=20

# Response
{
  "items": [
    {
      "id": "exception_id",
      "study_instance_uid": "1.2.840.113619.2.55.3.604688119.971.1234567890.123",
      "error_type": "storage_error",
      "error_message": "Connection timeout to storage backend",
      "created_at": "2025-09-08T10:30:00Z",
      "retry_count": 2,
      "status": "pending",
      "source_info": {
        "type": "c_store",
        "calling_ae": "MODALITY_1"
      }
    }
  ],
  "total": 15,
  "page": 1,
  "per_page": 20
}
```

#### Retry Exception
```bash
POST /api/v1/monitoring/exceptions/{exception_id}/retry
Authorization: Api-Key your_api_key

# Response
{
  "message": "Exception retry queued successfully",
  "task_id": "celery_task_id"
}
```

## Data Operations

### DICOM Upload (STOW-RS)

#### Single File Upload
```bash
POST /api/v1/dicomweb/studies
Authorization: Api-Key your_api_key
Content-Type: application/dicom

# Binary DICOM file data
```

#### Multipart Upload
```bash
POST /api/v1/dicomweb/studies
Authorization: Api-Key your_api_key
Content-Type: multipart/related; type="application/dicom"; boundary=boundary123

--boundary123
Content-Type: application/dicom

# DICOM file 1 binary data
--boundary123
Content-Type: application/dicom

# DICOM file 2 binary data
--boundary123--
```

### Data Browser

#### Query Sources
```bash
POST /api/v1/data-browser/query
Authorization: Api-Key your_api_key
Content-Type: application/json

{
  "sources": ["dicomweb-source-1", "dimse-qr-source-2"],
  "query": {
    "StudyDate": "20250908",
    "PatientID": "PAT123456",
    "Modality": "CT"
  },
  "level": "STUDY"
}

# Response
{
  "results": [
    {
      "source": "dicomweb-source-1",
      "studies": [
        {
          "StudyInstanceUID": "1.2.840.113619.2.55.3.604688119.971.1234567890.123",
          "StudyDate": "20250908",
          "StudyTime": "143000",
          "PatientID": "PAT123456",
          "PatientName": "DOE^JOHN",
          "Modality": "CT",
          "NumberOfStudyRelatedInstances": 450
        }
      ]
    }
  ],
  "query_time_ms": 234
}
```

## Error Handling

### Standard Error Response
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid DICOM tag format",
    "details": {
      "field": "tag",
      "value": "invalid_tag",
      "expected": "Format: (0xAAAA,0xBBBB)"
    }
  },
  "timestamp": "2025-09-08T10:30:00Z",
  "request_id": "req_1234567890"
}
```

### HTTP Status Codes
- `200 OK` - Successful operation
- `201 Created` - Resource created successfully
- `400 Bad Request` - Invalid request data
- `401 Unauthorized` - Authentication required
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource already exists
- `422 Unprocessable Entity` - Validation error
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Service temporarily unavailable

## Rate Limiting

API requests are rate limited based on authentication method:

- **OAuth Users**: 1000 requests per hour
- **API Keys**: 10000 requests per hour  
- **DICOM Uploads**: 100 GB per hour

Rate limit headers are included in responses:
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1694174400
```

## Pagination

List endpoints support pagination with these parameters:

- `page` - Page number (default: 1)
- `per_page` - Items per page (default: 50, max: 100)

Response includes pagination metadata:
```json
{
  "items": [...],
  "total": 1000,
  "page": 1, 
  "per_page": 50,
  "total_pages": 20,
  "has_next": true,
  "has_prev": false
}
```

## WebSocket Events

Real-time events are available via WebSocket connection:

```javascript
// Connect to WebSocket
const ws = new WebSocket('wss://axiom.yourdomain.com/ws');

// Authenticate
ws.send(JSON.stringify({
  type: 'auth',
  token: 'your_jwt_token'
}));

// Subscribe to events
ws.send(JSON.stringify({
  type: 'subscribe',
  events: ['study_processed', 'exception_created']
}));

// Handle events
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Event:', data);
};
```

---

This API reference provides comprehensive documentation for integrating with the Axiom platform. For interactive API exploration, visit the Swagger UI at `/api/v1/docs`.
