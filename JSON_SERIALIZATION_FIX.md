# JSON Serialization Error Fix

## Problem
The application was experiencing repeated JSON serialization errors when deleting orders:

```
TypeError: Object of type date is not JSON serializable
```

This occurred in `app/events.py` at line 150 when trying to publish order events to RabbitMQ.

## Root Cause Analysis
1. **Pydantic v1 vs v2 inconsistency**: The delete endpoint was using old Pydantic v1 syntax:
   ```python
   order_data = schemas.ImagingOrderRead.from_orm(db_order).dict()
   ```
   Instead of the v2 syntax that properly handles date serialization:
   ```python
   schemas.ImagingOrderRead.model_validate(order).model_dump(mode='json')
   ```

2. **Missing JSON serializer**: The `json.dumps()` calls in `events.py` had no fallback for non-serializable objects like `date` and `datetime`.

## Solution Implemented

### 1. Added Custom JSON Serializer (`app/events.py`)
```python
def json_serializer(obj):
    """
    Custom JSON serializer for objects not serializable by default json code.
    Handles datetime, date, and other common SQLAlchemy types.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Handle any other problematic types here
    return str(obj)
```

### 2. Updated All JSON Serialization Calls
- **events.py**: Added `default=json_serializer` to all `json.dumps()` calls
  - `publish_order_event()` function 
  - `publish_order_event_sync()` function
  - `SSEManager.broadcast()` method

### 3. Fixed Pydantic v2 Compatibility Issues
- **orders.py**: Updated delete endpoint to use proper v2 syntax:
  ```python
  order_data = schemas.ImagingOrderRead.model_validate(db_order).model_dump(mode='json')
  ```
- **crud_imaging_order.py**: Fixed status update event publishing

## Files Modified
1. `app/events.py` - Added custom JSON serializer and fixed all serialization calls
2. `app/api/api_v1/endpoints/orders.py` - Fixed delete endpoint Pydantic syntax
3. `app/crud/crud_imaging_order.py` - Fixed status update event publishing

## Benefits
- ✅ **No more JSON serialization errors**: All date/datetime objects are properly serialized
- ✅ **Robust error handling**: The custom serializer handles any problematic types
- ✅ **Pydantic v2 compliance**: All endpoints now use proper v2 syntax
- ✅ **Consistent serialization**: Same approach used across all event publishing

## Testing
Validated the fix with test payloads containing datetime and date objects:
- Custom serializer correctly converts dates to ISO format strings
- Full JSON serialization works without errors
- Event publishing should now work reliably

## Prevention
The custom `json_serializer` function will handle similar issues in the future and can be extended for other non-serializable types as needed.
