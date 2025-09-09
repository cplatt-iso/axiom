"""
Phase 3: Processing Pipeline Integration

GOAL: Ensure correlation_id flows through entire processing pipeline

KEY INTEGRATION POINTS:

1. Celery Task Propagation
   - Ensure correlation_id passes between Celery tasks
   - Maintain context across async operations

2. Storage Backend Operations
   - Include correlation_id in all storage operations
   - Track when objects are stored/moved/deleted

3. Rule Engine Application
   - Log which rules were applied with correlation_id
   - Track rule processing decisions

4. Error Handling & Dustbin
   - Maintain correlation_id when files fail processing
   - Track error disposition with correlation context

IMPLEMENTATION:

File: app/worker/tasks_base.py (new)
```python
from app.core.correlation import set_correlation_id

class CorrelatedTask(Task):
    """Base Celery task that maintains correlation context"""
    
    def __call__(self, *args, **kwargs):
        correlation_id = kwargs.pop('correlation_id', None)
        if correlation_id:
            set_correlation_id(correlation_id)
        return super().__call__(*args, **kwargs)
```

File: app/services/storage_backends/base.py
```python
def store(self, modified_ds, correlation_id=None, **kwargs):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    logger.info("Storing DICOM to backend", 
                backend=self.name,
                sop_instance_uid=modified_ds.SOPInstanceUID)
    
    # Backend-specific storage logic
    result = self._do_store(modified_ds, **kwargs)
    
    # Log storage result with correlation
    logger.info("Storage completed", 
                backend=self.name,
                status=result.get('status'))
    
    return result
```

File: app/services/rule_engine.py
```python
def apply_rules(self, dicom_data, correlation_id=None):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    applied_rules = []
    
    for rule in self.get_applicable_rules(dicom_data):
        logger.info("Applying processing rule", 
                    rule_id=rule.id,
                    rule_name=rule.name)
        
        result = rule.apply(dicom_data)
        
        # Store rule application in database
        rule_application = ProcessingRuleApplied(
            correlation_id=correlation_id,
            rule_id=rule.id,
            result=result,
            applied_at=datetime.utcnow()
        )
        
        applied_rules.append(rule_application)
    
    return applied_rules
```
"""
