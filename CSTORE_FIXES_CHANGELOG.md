# DICOM C-STORE API Fixes

## Issues Fixed

### 1. `send_c_store()` API Usage
**Problem**: Code was incorrectly calling `assoc.send_c_store(dataset, context_id=...)` which caused:
```
TypeError: Association.send_c_store() got an unexpected keyword argument 'context_id'
```

**Solution**: Updated to use the correct API `assoc.send_c_store(dataset)` without context_id parameter. Pynetdicom automatically selects the appropriate presentation context.

**Files Updated**:
- `app/services/network/dimse/scu_service.py` - Both `store_dataset()` and `store_datasets_batch()` functions
- `app/services/storage_backends/dicom_cstore.py` - Main C-STORE implementation
- `ROBUST_CSTORE_USAGE.md` - Updated documentation examples

### 2. Association.requested_contexts Attribute
**Problem**: Code was trying to access `association.requested_contexts` which doesn't exist in all pynetdicom versions, causing:
```
AttributeError: 'Association' object has no attribute 'requested_contexts'
```

**Solution**: Added defensive checks in `analyze_accepted_contexts()` to handle missing `requested_contexts` attribute gracefully.

**Files Updated**:
- `app/services/network/dimse/transfer_syntax_negotiation.py` - `analyze_accepted_contexts()` function

### 3. Transfer Syntax List Handling
**Problem**: `PresentationContext.transfer_syntax` can be either a string or list, causing display issues.

**Solution**: Added consistent handling throughout the codebase to check if transfer_syntax is a list and extract the first element for display purposes.

**Files Updated**:
- `app/services/network/dimse/transfer_syntax_negotiation.py` - Multiple functions
- `app/services/storage_backends/dicom_cstore.py` - Context analysis

## Testing
- Updated test script confirms all fixes work correctly
- No more API errors when calling `send_c_store()`
- Better error handling and logging throughout

## Impact
These fixes resolve the immediate C-STORE errors while maintaining all the robust transfer syntax negotiation and fallback capabilities.
