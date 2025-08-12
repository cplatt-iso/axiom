# DICOM Query Spanning API Reference

## Base URLs

- **Original API**: `http://localhost:8001`
- **Enterprise Coordinator**: `http://localhost:8002`

## Authentication

Currently using basic authentication. All endpoints require valid API credentials.

## Original API Endpoints (Port 8001)

### Spanner Configuration Management

#### List Spanner Configurations
```http
GET /spanner/configs
```

**Response:**
```json
[
  {
    "id": 1,
    "name": "Hospital Network Spanning",
    "description": "Query across all hospital PACS systems",
    "query_timeout_seconds": 300,
    "max_concurrent_queries": 10,
    "failure_strategy": "continue_on_partial_failure",
    "deduplication_strategy": "by_study_instance_uid",
    "priority": 1,
    "is_active": true,
    "created_at": "2025-08-11T19:30:00Z",
    "updated_at": "2025-08-11T19:30:00Z"
  }
]
```

#### Create Spanner Configuration
```http
POST /spanner/configs
Content-Type: application/json

{
  "name": "New Hospital Network",
  "description": "Spanning configuration for new hospital network",
  "query_timeout_seconds": 300,
  "max_concurrent_queries": 10,
  "failure_strategy": "continue_on_partial_failure",
  "deduplication_strategy": "by_study_instance_uid",
  "priority": 1,
  "is_active": true
}
```

#### Get Spanner Configuration
```http
GET /spanner/configs/{config_id}
```

#### Update Spanner Configuration
```http
PUT /spanner/configs/{config_id}
Content-Type: application/json

{
  "name": "Updated Configuration",
  "description": "Updated description",
  "is_active": false
}
```

#### Delete Spanner Configuration
```http
DELETE /spanner/configs/{config_id}
```

### Source Management

#### List Sources
```http
GET /spanner/sources
```

**Response:**
```json
[
  {
    "id": 1,
    "spanner_config_id": 1,
    "name": "Main Hospital PACS",
    "source_type": "dimse",
    "connection_config": {
      "ae_title": "MAIN_PACS",
      "host": "192.168.1.100",
      "port": 104,
      "calling_ae_title": "AXIOM_SPANNER"
    },
    "query_config": {
      "max_results_per_query": 1000,
      "timeout_seconds": 60
    },
    "is_active": true,
    "priority": 1
  }
]
```

#### Create Source
```http
POST /spanner/sources
Content-Type: application/json

{
  "spanner_config_id": 1,
  "name": "New PACS System",
  "source_type": "dimse",
  "connection_config": {
    "ae_title": "NEW_PACS",
    "host": "192.168.1.200",
    "port": 104,
    "calling_ae_title": "AXIOM_SPANNER"
  },
  "query_config": {
    "max_results_per_query": 1000,
    "timeout_seconds": 60
  },
  "is_active": true,
  "priority": 1
}
```

### Query Log Management

#### List Query Logs
```http
GET /spanner/query-logs
```

#### Get Query Log Details
```http
GET /spanner/query-logs/{log_id}
```

## Enterprise Coordinator API (Port 8002)

### Health Check

#### Service Health
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "spanner-coordinator"
}
```

### Spanning Query Operations

#### Create Spanning Query
```http
POST /spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY&requesting_ae_title=TEST_AE&requesting_ip=192.168.1.100
Content-Type: application/json

{
  "PatientID": "12345",
  "PatientName": "DOE^JOHN",
  "StudyDate": "20240101-20241231",
  "StudyDescription": "*MRI*",
  "Modality": "MR"
}
```

**Parameters:**
- `spanner_config_id` (required): ID of the spanner configuration to use
- `query_type` (required): Type of DICOM query (`C-FIND`, `C-MOVE`, `C-GET`)
- `query_level` (optional): Query level (`PATIENT`, `STUDY`, `SERIES`, `IMAGE`) - defaults to `STUDY`
- `requesting_ae_title` (optional): AE title of the requesting system
- `requesting_ip` (optional): IP address of the requesting system

**Request Body:** DICOM query filters as JSON object

**Response:**
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "message": "Spanning query accepted for processing"
}
```

#### Get Query Status
```http
GET /spanning-query/{query_id}
```

**Response (In Progress):**
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "in_progress",
  "created_at": "2025-08-11T20:30:00Z",
  "partial_results_count": 45,
  "sources_responded": 3,
  "total_sources": 8,
  "estimated_completion": "2025-08-11T20:32:00Z"
}
```

**Response (Completed):**
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "created_at": "2025-08-11T20:30:00Z",
  "completed_at": "2025-08-11T20:31:45Z",
  "execution_time_ms": 105000,
  "total_results": 150,
  "sources_responded": 8,
  "sources_failed": 0,
  "results": [
    {
      "source_name": "Main Hospital PACS",
      "source_id": 1,
      "result_count": 25,
      "execution_time_ms": 1500,
      "studies": [
        {
          "StudyInstanceUID": "1.2.3.4.5.6.7.8.9",
          "PatientID": "12345",
          "PatientName": "DOE^JOHN",
          "StudyDate": "20240315",
          "StudyDescription": "MRI Brain",
          "Modality": "MR",
          "SeriesCount": 4,
          "ImageCount": 120
        }
      ]
    }
  ]
}
```

**Response (Failed):**
```json
{
  "query_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "created_at": "2025-08-11T20:30:00Z",
  "failed_at": "2025-08-11T20:31:00Z",
  "error": "Connection timeout to primary PACS system",
  "partial_results": 25,
  "sources_responded": 3,
  "sources_failed": 5
}
```

## Common Query Examples

### Patient Search
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=PATIENT" \
  -H "Content-Type: application/json" \
  -d '{
    "PatientID": "12345",
    "PatientName": "DOE^JOHN"
  }'
```

### Study Search by Date Range
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY" \
  -H "Content-Type: application/json" \
  -d '{
    "PatientID": "12345",
    "StudyDate": "20240101-20241231",
    "Modality": "CT"
  }'
```

### Study Search by Description
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=STUDY" \
  -H "Content-Type: application/json" \
  -d '{
    "StudyDescription": "*CHEST*",
    "StudyDate": "20240801-"
  }'
```

### Series Search
```bash
curl -X POST "http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND&query_level=SERIES" \
  -H "Content-Type: application/json" \
  -d '{
    "StudyInstanceUID": "1.2.3.4.5.6.7.8.9",
    "Modality": "CT",
    "SeriesDescription": "*AXIAL*"
  }'
```

## Error Responses

### Standard Error Format
```json
{
  "error": "error_code",
  "message": "Human readable error message",
  "details": {
    "field": "specific error details"
  },
  "timestamp": "2025-08-11T20:30:00Z"
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `validation_error` | 400 | Invalid request parameters |
| `config_not_found` | 404 | Spanner configuration not found |
| `query_not_found` | 404 | Query ID not found |
| `source_unavailable` | 503 | One or more sources unavailable |
| `timeout_exceeded` | 408 | Query execution timeout |
| `internal_error` | 500 | Internal server error |

## Rate Limiting

- **Original API**: 1000 requests per minute per IP
- **Enterprise API**: 100 spanning queries per minute per IP
- **Headers**: Rate limit info included in response headers

## WebSocket Support (Future)

Real-time query status updates will be available via WebSocket:

```javascript
const ws = new WebSocket('ws://localhost:8002/ws/spanning-query/{query_id}');
ws.onmessage = function(event) {
  const status = JSON.parse(event.data);
  console.log('Query status:', status);
};
```

## SDK Examples

### Python SDK
```python
import requests

# Create spanning query
response = requests.post(
    'http://localhost:8002/spanning-query',
    params={
        'spanner_config_id': 1,
        'query_type': 'C-FIND',
        'query_level': 'STUDY'
    },
    json={
        'PatientID': '12345',
        'StudyDate': '20240101-20241231'
    }
)

query = response.json()
query_id = query['query_id']

# Poll for results
import time
while True:
    status = requests.get(f'http://localhost:8002/spanning-query/{query_id}').json()
    if status['status'] in ['completed', 'failed']:
        break
    time.sleep(1)

print(f"Query completed with {status.get('total_results', 0)} results")
```

### JavaScript SDK
```javascript
async function createSpanningQuery(filters) {
  const response = await fetch('http://localhost:8002/spanning-query?spanner_config_id=1&query_type=C-FIND', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(filters)
  });
  
  const query = await response.json();
  return query.query_id;
}

async function pollQueryStatus(queryId) {
  while (true) {
    const response = await fetch(`http://localhost:8002/spanning-query/${queryId}`);
    const status = await response.json();
    
    if (status.status === 'completed' || status.status === 'failed') {
      return status;
    }
    
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}
```
