# DICOM Modality IP Bypass Validation

## Overview

The DICOM Modality Worklist (DMWL) service includes a feature to bypass IP address validation for specific modalities. This is particularly useful for:

- **Testing environments** where modalities run in containers with dynamic IP addresses
- **Development setups** where IP addresses change frequently
- **Special "magic" AE titles** that need to access DMWL from multiple locations

## How It Works

### Normal Security Model
By default, DMWL access requires:
1. Valid AE Title that matches a configured modality
2. IP address that matches the configured IP for that modality
3. Modality must be active and DMWL-enabled
4. Associated facility must be active

### Bypass Security Model
When `bypass_ip_validation=True` is set for a modality:
1. Valid AE Title that matches a configured modality ✅
2. **IP address validation is skipped** ⚠️
3. Modality must be active and DMWL-enabled ✅
4. Associated facility must be active ✅

## Database Schema

The `modalities` table includes a new field:

```sql
bypass_ip_validation BOOLEAN NOT NULL DEFAULT FALSE
```

## API Usage

### Creating a Modality with IP Bypass

```python
from app.schemas.modality import ModalityCreate

modality_data = ModalityCreate(
    name="Test Container CT",
    ae_title="TEST_CT_BYPASS",
    ip_address="192.168.1.100",  # This will be ignored due to bypass
    modality_type="CT",
    facility_id=1,
    is_active=True,
    is_dmwl_enabled=True,
    bypass_ip_validation=True,  # Enable IP bypass
    # ... other fields
)
```

### Updating Existing Modality to Enable Bypass

```python
from app.schemas.modality import ModalityUpdate

update_data = ModalityUpdate(
    bypass_ip_validation=True
)
```

## Testing Script

Use the provided script to create test modalities:

```bash
# Create a test modality with IP bypass
python create_test_modality.py

# List all modalities with IP bypass enabled
python create_test_modality.py list
```

## Security Considerations

⚠️ **Important Security Notes:**

1. **Use Sparingly**: Only enable bypass for testing or specific use cases
2. **Monitor Access**: Check logs for bypass usage with `DMWL_ACCESS_GRANTED_VIA_IP_BYPASS`
3. **Network Security**: Ensure proper network segmentation if using bypass
4. **Audit Trail**: All access attempts are logged with detailed context

## Logging

The system provides detailed logging for bypass scenarios:

### Normal Access
```
DMWL_ACCESS_GRANTED: modality_id=123, modality_type=CT
```

### Bypass Access
```
DMWL_ACCESS_GRANTED_VIA_IP_BYPASS: modality_id=123, modality_type=CT, configured_ip=192.168.1.100, actual_ip=172.17.0.5
```

### Access Denied
```
DMWL_ACCESS_DENIED_IP_MISMATCH: configured_ip=192.168.1.100, actual_ip=172.17.0.5, modality_id=123
```

## Implementation Details

### CRUD Layer (`app/crud/crud_modality.py`)

The `can_query_dmwl()` function implements a two-step validation:

1. **Try normal validation**: AE title + IP address match
2. **Try bypass validation**: AE title + `bypass_ip_validation=True`

### Handler Layer (`app/services/network/dimse/handlers.py`)

The validation function returns enhanced modality info including:
- `bypass_ip_validation`: Whether bypass is enabled
- `configured_ip`: The configured IP address
- `actual_ip`: The actual requesting IP address

## Example Use Cases

### Container Testing
```python
# Create a modality for testing in Docker
modality = ModalityCreate(
    name="Docker Test CT",
    ae_title="DOCKER_CT_01",
    ip_address="192.168.1.100",  # Static config IP
    bypass_ip_validation=True,   # Allow dynamic container IPs
    # ... other fields
)
```

### Multi-Location Modality
```python
# Modality that can connect from multiple networks
modality = ModalityCreate(
    name="Mobile CT Unit",
    ae_title="MOBILE_CT_01",
    ip_address="10.0.0.100",     # Primary location IP
    bypass_ip_validation=True,   # Allow from other locations
    # ... other fields
)
```

## Migration

The database migration `123456789abc_add_bypass_ip_validation_to_modalities.py` adds the new field with:
- Default value: `false` (secure by default)
- Index on the field for performance
- Non-nullable with server default

## Testing

Comprehensive tests cover:
- Normal access validation
- Bypass access validation
- Security boundary testing
- CRUD layer functionality
- Handler layer integration

Run tests with:
```bash
pytest app/tests/services/test_modality_dmwl_validation.py
pytest app/tests/crud/test_modality_bypass.py
```
