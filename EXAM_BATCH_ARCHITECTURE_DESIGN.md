# Exam Batch Architecture Design

## Problem Statement
The current system processes individual DICOM instances and sends each via separate DIMSE associations, which is inefficient and creates excessive network overhead. We need to batch complete studies (exams) and send all instances in a single association.

## Current Flow Issues
```
DICOM files → individual processing → individual sending (one association per file) ❌
```

## Target Architecture
```
DICOM files → batch processing → study-level sending (one association per study) ✅
```

## Key Components

### 1. Enhanced Exam Batching
- `exam_batches` table tracks studies by destination
- `exam_batch_instances` table tracks individual files in each batch
- Batch status: `PENDING` → `READY` → `SENDING` → `SENT` / `FAILED`

### 2. Study Completion Detection
- Need logic to detect when all instances of a study have been processed
- Mark batch as `READY` for sending when complete

### 3. Batch Sender Service
- New service that processes `READY` exam batches
- Sends all instances in a study via single DIMSE association
- Updates batch status appropriately

### 4. Study Completion Strategy
Two approaches for detecting complete studies:

#### Option A: Time-based (Current approach)
- Mark batch as READY after X seconds of no new instances
- Pros: Simple, handles unknown study sizes
- Cons: Introduces delay, may miss late arrivals

#### Option B: MPPS-based (Future enhancement)
- Use Modality Performed Procedure Step (MPPS) to know expected instance count
- Mark as READY when expected count reached
- Pros: Immediate completion, accurate
- Cons: Requires MPPS integration

### 5. Implementation Steps

1. **Fix Worker Task** - Stop individual file queuing for C-STORE destinations
2. **Add Batch Completion Logic** - Detect when studies are complete
3. **Create Batch Sender Service** - Process READY batches
4. **Update dcm4che/pynetdicom Senders** - Handle batch jobs instead of individual files
5. **Add Monitoring/Observability** - Track batch processing metrics

## Database Schema (Already Implemented)

```sql
-- Studies grouped by destination
CREATE TABLE exam_batches (
    id INTEGER PRIMARY KEY,
    study_instance_uid VARCHAR NOT NULL,
    destination_id INTEGER NOT NULL REFERENCES storage_backend_configs(id),
    status VARCHAR NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Individual instances in each batch
CREATE TABLE exam_batch_instances (
    id INTEGER PRIMARY KEY,
    batch_id INTEGER NOT NULL REFERENCES exam_batches(id),
    processed_filepath VARCHAR NOT NULL,
    sop_instance_uid VARCHAR NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Benefits

1. **Reduced Network Overhead** - Single association per study instead of per instance
2. **Better PACS Compatibility** - More natural workflow for receiving systems
3. **Improved Performance** - Bulk operations are more efficient
4. **Easier Troubleshooting** - Study-level success/failure tracking
5. **Scalability** - Better handling of high-volume imaging centers

## Monitoring Considerations

- Track average batch size
- Monitor batch completion times
- Alert on stuck PENDING batches
- Track association success rates per destination
