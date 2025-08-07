# Transfer Syntax Negotiation Usage Examples

## Example 1: Basic C-STORE with Conservative Strategy

```python
from app.services.storage_backends.dicom_cstore import CStoreStorage

# Configure with conservative strategy (default)
config = {
    "name": "PACS_STORE",
    "remote_ae_title": "REMOTE_PACS",
    "remote_host": "192.168.1.100", 
    "remote_port": 11112,
    "local_ae_title": "AXIOM_SCU",
    "transfer_syntax_strategy": "conservative",  # Most compatible
    "max_association_retries": 3
}

backend = CStoreStorage(config)
result = backend.store(dataset)
```

## Example 2: Progressive Strategy with Fallback

```python
# Start with compression support, fallback to conservative
config = {
    "name": "MODERN_PACS",
    "remote_ae_title": "MODERN_PACS",
    "remote_host": "pacs.hospital.com",
    "remote_port": 11112,
    "transfer_syntax_strategy": "compression",  # Will try: compression -> standard -> conservative -> universal
}

backend = CStoreStorage(config)
result = backend.store(dataset)

# Result includes strategy used:
print(f"Used strategy: {result['strategy_used']}")
print(f"Attempts made: {result['attempts_made']}")
```

## Example 3: Manual Presentation Context Creation

```python
from app.services.network.dimse.transfer_syntax_negotiation import (
    create_presentation_contexts_with_fallback,
    TransferSyntaxStrategy
)

# Create contexts for multiple strategies
contexts = create_presentation_contexts_with_fallback(
    sop_class_uid="1.2.840.10008.5.1.4.1.1.2",  # CT Image Storage
    strategies=["conservative", "universal"],
    max_contexts_per_strategy=3
)

# Use with pynetdicom AE
ae = AE()
for context in contexts:
    ae.add_requested_context(context.abstract_syntax, context.transfer_syntax)
```

## Available Strategies

1. **"universal"** - Only Implicit VR Little Endian (maximum compatibility)
2. **"conservative"** - Implicit + Explicit VR Little Endian (recommended default)
3. **"standard"** - Conservative + Explicit VR Big Endian
4. **"compression"** - Standard + RLE and JPEG 2000 Lossless
5. **"extended"** - All formats including lossy compression

## Error Handling

```python
try:
    result = backend.store(dataset)
except StorageBackendError as e:
    # Check if error includes recommendations
    if hasattr(e, 'recommendation'):
        print("Recommended transfer syntaxes:", e.recommendation)
```

## Analyzing Association Results

```python
from app.services.network.dimse.transfer_syntax_negotiation import analyze_accepted_contexts

# After establishing association
analysis = analyze_accepted_contexts(association)
print(f"Accepted contexts: {analysis['accepted_count']}")
print(f"Peer supports compression: {analysis['peer_supports_compression']}")

for ctx in analysis['accepted_contexts']:
    print(f"Context {ctx['context_id']}: {ctx['sop_name']} - {ctx['transfer_syntax_name']}")
```

## Best Practices

1. **Start Conservative**: Use "conservative" strategy for new PACS connections
2. **Monitor Logs**: Check which strategy succeeds for each PACS
3. **Configure Per PACS**: Different PACS may need different strategies
4. **Test Thoroughly**: Validate with your specific PACS before production

## Troubleshooting Common Issues

### "No presentation context accepted"
- Try "universal" strategy (Implicit VR only)
- Check PACS configuration for supported transfer syntaxes

### "Transfer syntax not supported"  
- PACS may only support older formats
- Use "conservative" or "universal" strategies

### "Association rejected"
- Check AE titles, IP addresses, ports
- Verify PACS allows connections from your system
