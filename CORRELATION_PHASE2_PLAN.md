"""
Phase 2: Entry Point Integration

CRITICAL ENTRY POINTS FOR CORRELATION ID INJECTION:

1. STOW-RS API (/api/v1/dicomweb/studies)
   - Generate correlation_id when DICOM data enters via STOW-RS
   - Pass to processing tasks
   - Store in database records

2. DICOM Listener (SCP)
   - Generate correlation_id when DICOM received via C-STORE
   - Bind to all subsequent processing

3. JSON API uploads
   - Generate correlation_id for programmatic uploads
   - Track through processing pipeline

4. DICOMweb Polling
   - Generate correlation_id when polling external sources
   - Track retrieved studies through processing

IMPLEMENTATION POINTS:

File: app/api/api_v1/endpoints/dicomweb.py
```python
from app.core.correlation import set_correlation_id, generate_correlation_id

@router.post("/studies")
async def stow_rs_store(request: Request, ...):
    # Generate correlation ID for this STOW-RS request
    correlation_id = generate_correlation_id()
    set_correlation_id(correlation_id)
    
    logger.info("STOW-RS request received", 
                correlation_id=correlation_id,
                source_ip=source_ip)
    
    # Pass correlation_id to processing tasks
    for dicom_part in dicom_parts:
        task_result = process_dicom_task.delay(
            temp_filepath, 
            association_info,
            correlation_id=correlation_id  # NEW
        )
```

File: services/listeners/dicom_listener.py
```python
def handle_store(self, event):
    # Generate correlation ID when DICOM arrives
    correlation_id = generate_correlation_id()
    set_correlation_id(correlation_id)
    
    # All subsequent processing uses this ID
    logger.info("DICOM C-STORE received", 
                correlation_id=correlation_id,
                calling_ae=event.assoc.requestor.ae_title)
```

File: app/worker/processing_tasks.py
```python
@app.task(bind=True)
def process_dicom_task(self, filepath, association_info, correlation_id=None):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    # All processing logs will include correlation_id
    logger.info("Starting DICOM processing", file=filepath)
    
    # Store correlation_id in database
    task_record = ProcessingTask(
        correlation_id=correlation_id,
        file_path=filepath,
        # ... other fields
    )
```
"""
