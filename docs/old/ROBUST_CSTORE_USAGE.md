# Robust DICOM C-STORE Implementation

This implementation provides enhanced DICOM C-STORE functionality with robust transfer syntax negotiation and error handling.

## Key Features

1. **Progressive Transfer Syntax Fallback**: Automatically tries different transfer syntax strategies if the initial one fails
2. **Detailed Error Analysis**: Provides specific error information when C-STORE operations fail
3. **Batch Operations**: Support for storing multiple datasets in a single association
4. **Smart Context Selection**: Automatically selects the best presentation context for each dataset

## Transfer Syntax Strategies

The implementation supports several strategies, listed from most conservative to most extended:

- `universal`: Only Implicit VR Little Endian (maximum compatibility)
- `conservative`: Implicit + Explicit VR Little Endian
- `standard`: Conservative + Explicit VR Big Endian  
- `compression`: Standard + RLE and JPEG 2000 Lossless
- `extended`: All supported transfer syntaxes including lossy compression

## Usage Examples

### Basic C-STORE Operation

```python
from app.services.network.dimse.scu_service import store_dataset

config = {
    "remote_host": "pacs.example.com",
    "remote_port": 11112,
    "remote_ae_title": "PACS_SCP",
    "local_ae_title": "AXIOM_SCU",
    "tls_enabled": False
}

# Store a single dataset with automatic fallback
result = store_dataset(
    config=config,
    dataset=my_dicom_dataset,
    transfer_syntax_strategy="conservative",  # Start conservative
    max_retries=3
)

if result["status"] == "success":
    print(f"Stored successfully using {result['strategy_used']} strategy")
    print(f"Transfer syntax: {result['transfer_syntax']}")
else:
    print(f"Store failed: {result['message']}")
```

### Batch C-STORE Operation

```python
from app.services.network.dimse.scu_service import store_datasets_batch

# Store multiple datasets in one association
result = store_datasets_batch(
    config=config,
    datasets=[dataset1, dataset2, dataset3],
    transfer_syntax_strategy="standard",
    continue_on_error=True  # Continue even if some datasets fail
)

print(f"Batch results: {result['successful']} successful, {result['failed']} failed")
for detail in result['details']:
    print(f"  Dataset {detail['dataset_index']}: {detail['status']}")
```

### Advanced Association Management

```python
from app.services.network.dimse.scu_service import manage_association_with_fallback
from app.services.network.dimse.transfer_syntax_negotiation import find_compatible_transfer_syntax

# For custom operations with manual control
with manage_association_with_fallback(
    remote_host="pacs.example.com",
    remote_port=11112,
    remote_ae_title="PACS_SCP",
    sop_class_uid="1.2.840.10008.5.1.4.1.1.2",  # CT Image Storage
    transfer_syntax_strategy="conservative"
) as (assoc, metadata):
    
    # Find best context for your dataset
    context, reason = find_compatible_transfer_syntax(
        dataset=my_dataset,
        accepted_contexts=assoc.accepted_contexts,
        sop_class_uid=str(my_dataset.SOPClassUID)
    )
    
    if context:
        # Perform C-STORE (pynetdicom automatically selects the right context)
        status = assoc.send_c_store(my_dataset)
        print(f"C-STORE status: {status.Status if status else 'No response'}")
```

## Error Handling

The implementation provides detailed error information when operations fail:

```python
try:
    result = store_dataset(config, dataset)
except DimseCommandError as e:
    print(f"DICOM command failed: {e}")
    print(f"Remote AE: {e.remote_ae}")
    print(f"Details: {e.details}")
except AssociationError as e:
    print(f"Association failed: {e}")
except TlsConfigError as e:
    print(f"TLS configuration error: {e}")
```

## Troubleshooting Common Issues

### "No presentation context accepted" errors

This usually means the PACS doesn't support the requested transfer syntax. Try:

1. Use a more conservative strategy: `"universal"` or `"conservative"`
2. Check the error analysis in the exception details
3. Verify the SOP class is supported by the target PACS

### Transfer syntax mismatch errors

When you get errors like "No presentation context for 'CT Image Storage' has been accepted by the peer with 'Explicit VR Little Endian'":

1. The dataset requires a specific transfer syntax that the PACS doesn't accept
2. Use `find_compatible_transfer_syntax()` to see what the PACS actually supports
3. Consider converting the dataset to a supported transfer syntax before sending

### Performance optimization

For high-volume operations:

1. Use `store_datasets_batch()` instead of individual stores
2. Reuse associations when possible
3. Start with `"conservative"` strategy to reduce negotiation time
4. Use `continue_on_error=True` for batch operations

## Configuration Options

The `config` dictionary supports these options:

- `remote_host`: Target PACS hostname/IP (required)
- `remote_port`: Target PACS port (required) 
- `remote_ae_title`: Target PACS AE title (required)
- `local_ae_title`: Local AE title (default: "AXIOM_SCU")
- `tls_enabled`: Enable TLS encryption (default: False)
- `tls_ca_cert_secret_name`: GCP Secret Manager ID for CA certificate
- `tls_client_cert_secret_name`: GCP Secret Manager ID for client certificate
- `tls_client_key_secret_name`: GCP Secret Manager ID for client private key

This robust implementation should handle most PACS compatibility issues and provide clear feedback when problems occur.
