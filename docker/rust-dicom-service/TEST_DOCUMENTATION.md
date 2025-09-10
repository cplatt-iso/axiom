# Axiom Rust DICOM Service Test Suite

This document describes the comprehensive test suite for the Axiom Rust DICOM service.

## Overview

The test suite provides comprehensive coverage for the Rust DICOM listener service, including:

- **Unit Tests**: Testing individual components and functions
- **Integration Tests**: Testing component interactions and full workflows
- **Benchmarks**: Performance testing for critical operations
- **Configuration Tests**: Environment variable handling and validation

## Test Structure

```
src/
├── config_tests.rs      # Configuration management tests
├── listener_tests.rs    # Directory watcher and DICOM processing tests
├── main_tests.rs        # CLI and main application tests
├── lib.rs              # Library module definitions
├── config.rs           # Configuration module
├── listener.rs         # Directory watcher implementation
└── main.rs             # Main application

tests/
└── integration_test.rs  # End-to-end integration tests

benches/
└── performance_tests.rs # Performance benchmarks
```

## Test Coverage

### Configuration Tests (`config_tests.rs`)
- ✅ Environment variable parsing with defaults
- ✅ Environment variable parsing with custom values
- ✅ Invalid configuration handling (ports, timeouts, etc.)
- ✅ RabbitMQ URL generation
- ✅ Configuration serialization/deserialization

### Listener Tests (`listener_tests.rs`)
- ✅ DirectoryWatcher creation and initialization
- ✅ DICOM file detection in study directories
- ✅ Study directory scanning with multiple studies
- ✅ Study filtering (ignores non-DICOM directories)
- ✅ Study processing workflow
- ✅ Timeout-based study processing
- ✅ Active study management

### Main Application Tests (`main_tests.rs`)
- ✅ Socket address parsing and validation
- ✅ Instance ID fallback logic
- ✅ CLI parameter validation (AE titles, etc.)
- ✅ Directory creation for DICOM storage
- ✅ Signal handling setup
- ✅ Configuration integration

### Integration Tests (`integration_test.rs`)
- ✅ Full workflow testing (config → watcher → processing)
- ✅ RabbitMQ URL integration
- ✅ Study timeout integration workflow

### Benchmarks (`performance_tests.rs`)
- ✅ Configuration creation performance
- ✅ RabbitMQ URL generation performance
- ✅ Directory scanning performance

## Running Tests

### Quick Test Run
```bash
cargo test --features test-utils -- --test-threads=1
```

### Comprehensive Test Suite
```bash
./run_tests.sh
```

### Individual Test Categories
```bash
# Unit tests only
cargo test --features test-utils --lib -- --test-threads=1

# Integration tests only
cargo test --features test-utils --test '*'

# Benchmarks only
cargo bench
```

## Performance Benchmarks

The benchmark results show excellent performance characteristics:

- **Config creation**: ~1µs average (efficient for frequent use)
- **RabbitMQ URL generation**: ~215ns average (very fast for high-frequency operations)
- **Directory scanning**: ~195µs average (acceptable performance that scales with file count)

## Test Features

### Environment Variable Isolation
Tests use single-threaded execution (`--test-threads=1`) to prevent environment variable conflicts between tests.

### Temporary Directory Management
All file system tests use `tempfile::TempDir` for proper cleanup and isolation.

### Async Testing
Async tests use `#[tokio::test]` for proper async runtime management.

### Test Utilities
The `test-utils` feature enables additional methods for testing internal state:
- `active_studies_len()`: Check number of active studies
- `add_active_study()`: Manually add studies for testing
- `instance_id()` and `config()`: Access internal configuration

## Test Quality Metrics

- **Total Tests**: 31 tests (28 unit + 3 integration)
- **Test Success Rate**: 100% pass rate
- **Coverage Areas**: Configuration, File Processing, CLI, Integration
- **Performance Validated**: Yes, with benchmarks
- **Memory Safety**: Validated through Rust's ownership system

## Continuous Integration

The test suite is designed for CI/CD integration:

1. **Fast Execution**: All tests complete in under 5 seconds
2. **No External Dependencies**: Tests use mock data and temporary directories
3. **Comprehensive Coverage**: Tests all major functionality paths
4. **Clear Output**: Detailed reporting with colored output
5. **Exit Codes**: Proper exit codes for CI/CD integration

## Adding New Tests

When adding new functionality:

1. **Add unit tests** in the appropriate `*_tests.rs` file
2. **Add integration tests** if the feature affects multiple components
3. **Add benchmarks** if the feature is performance-critical
4. **Update this documentation** with any new test categories

### Test Naming Convention
- `test_[component]_[functionality]_[scenario]`
- Examples: `test_config_from_env_with_defaults`, `test_listener_scan_multiple_studies`

### Test Structure
```rust
#[test]  // or #[tokio::test] for async
fn test_descriptive_name() {
    // Arrange: Set up test data
    let input = create_test_input();
    
    // Act: Execute the functionality
    let result = function_under_test(input);
    
    // Assert: Verify the results
    assert_eq!(result.expected_field, expected_value);
}
```

## Dependencies

### Test Dependencies
- `tokio-test`: Async testing utilities
- `tempfile`: Temporary directory management
- `assert_fs`: File system assertions
- `predicates`: Advanced assertion predicates

### Development Tools
- `cargo-fmt`: Code formatting (optional)
- `cargo-clippy`: Linting (optional)
- `cargo-tarpaulin`: Coverage reporting (optional)
- `cargo-audit`: Security auditing (optional)

## Security Considerations

Tests validate:
- Input sanitization for environment variables
- Proper handling of file paths
- No hardcoded secrets or credentials
- Secure temporary file handling

---

## Summary

This comprehensive test suite ensures the Axiom Rust DICOM service is:
- **Reliable**: Thoroughly tested functionality
- **Maintainable**: Clear test structure and documentation
- **Performant**: Validated through benchmarks
- **Secure**: Input validation and secure practices
- **Production-Ready**: CI/CD integration support

The test suite provides confidence for deployment and future development of the Rust DICOM listener service.
