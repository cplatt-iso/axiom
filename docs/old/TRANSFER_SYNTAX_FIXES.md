# Transfer Syntax List Handling Fixes

## Problem Identified

The "unhashable type: 'list'" error was occurring because the pynetdicom library returns `transfer_syntax` as a list in `PresentationContext` objects, but the code was trying to use these lists in contexts where single string values were expected.

## Recent Issue: 'Association' object has no attribute 'requested_contexts'

**New Error:** `"'Association' object has no attribute 'requested_contexts'"`

**Root Cause:** The pynetdicom `Association` object doesn't expose a `requested_contexts` attribute. This attribute exists on the `AE` (ApplicationEntity) object before association but not on the established association object.

**Fix Applied:** Added defensive checking with `hasattr()` to safely access `requested_contexts` only if it exists:

```python
if hasattr(association, 'requested_contexts') and association.requested_contexts:
    # Process requested contexts
else:
    # Log that we can't determine rejected contexts
    logger.debug("Association object does not have requested_contexts attribute or it's empty")
```

## Root Cause

In pynetdicom, when presentation contexts are accepted by a peer, the `transfer_syntax` attribute contains a list of transfer syntax UIDs, even if there's only one. The original code was:

1. Comparing `ctx.transfer_syntax == string` (list == string comparison)
2. Using `ctx.transfer_syntax` directly in string formatting
3. Passing lists to functions expecting strings

## Fixes Applied

### 1. Fixed `find_compatible_transfer_syntax()` function

**Before:**
```python
if ctx.transfer_syntax == dataset_ts:
```

**After:**
```python
ctx_transfer_syntaxes = ctx.transfer_syntax if isinstance(ctx.transfer_syntax, list) else [ctx.transfer_syntax]
if dataset_ts in ctx_transfer_syntaxes:
```

### 2. Fixed `analyze_accepted_contexts()` function

**Before:**
```python
"transfer_syntax": context.transfer_syntax,
"transfer_syntax_name": _get_transfer_syntax_name(context.transfer_syntax)
```

**After:**
```python
ctx_ts = context.transfer_syntax
if isinstance(ctx_ts, list):
    ts_for_display = ctx_ts[0] if ctx_ts else "Unknown"
else:
    ts_for_display = ctx_ts

"transfer_syntax": ts_for_display,
"transfer_syntax_name": _get_transfer_syntax_name(ts_for_display)
```

**Additional Fix:**
```python
# Safely check for requested_contexts attribute
if hasattr(association, 'requested_contexts') and association.requested_contexts:
    # Process rejected contexts
else:
    logger.debug("Association object does not have requested_contexts attribute")
```

### 3. Fixed `handle_cstore_context_error()` function

Similar list handling was applied to properly extract the first transfer syntax from the list for display purposes.

### 4. Fixed `dicom_cstore.py` storage backend

**Before:**
```python
accepted_ts = [ctx.transfer_syntax for ctx in assoc.accepted_contexts 
              if ctx.abstract_syntax == sop_class_uid]
```

**After:**
```python
accepted_ts = []
for ctx in assoc.accepted_contexts:
    if ctx.abstract_syntax == sop_class_uid:
        if isinstance(ctx.transfer_syntax, list):
            accepted_ts.extend(ctx.transfer_syntax)
        else:
            accepted_ts.append(ctx.transfer_syntax)
```

### 5. Fixed logging statements

Updated all places where `transfer_syntax` was logged to handle the list format properly.

## How It Works Now

1. **Robust List Handling**: All functions now properly handle `transfer_syntax` as either a list or a single string
2. **Safe Comparisons**: When checking if a transfer syntax is supported, the code now checks if it's contained in the list
3. **Proper Display**: For logging and error messages, the code extracts the first (and usually only) transfer syntax from the list
4. **Backward Compatibility**: The fixes work regardless of whether pynetdicom returns a list or a single string
5. **Safe Attribute Access**: Uses `hasattr()` to safely check for optional attributes on Association objects

## Testing

The fixes have been tested with:
- Mock presentation contexts that mimic pynetdicom's behavior
- Various transfer syntax strategies
- Error handling scenarios
- Missing `requested_contexts` attribute scenarios

## Expected Results

With these fixes, your C-STORE operations should now:
1. ✅ No longer throw "unhashable type: 'list'" errors
2. ✅ No longer throw "'Association' object has no attribute 'requested_contexts'" errors
3. ✅ Properly negotiate transfer syntaxes with PACS systems
4. ✅ Provide clear error messages when negotiations fail
5. ✅ Handle both conservative and extended transfer syntax strategies
6. ✅ Gracefully handle missing optional attributes on Association objects

The errors you saw in the logs should be resolved, and the C-STORE operation should proceed to actually attempt the DICOM transfer to your PACS system.
