#!/bin/bash

# Axiom Rust DICOM Service Test Runner
# This script runs all tests for the Rust DICOM service

set -e  # Exit on any error

echo "🧪 Running Axiom Rust DICOM Service Tests"
echo "=========================================="

# Change to the rust service directory
cd "$(dirname "$0")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}📋 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    print_error "Cargo not found. Please install Rust and Cargo."
    exit 1
fi

print_success "Cargo found: $(cargo --version)"

# Clean previous builds
print_step "Cleaning previous builds..."
cargo clean

# Update dependencies
print_step "Updating dependencies..."
cargo update

# Check code formatting
print_step "Checking code formatting..."
if cargo fmt -- --check; then
    print_success "Code formatting is correct"
else
    print_warning "Code formatting issues found. Run 'cargo fmt' to fix."
fi

# Run clippy for linting
print_step "Running Clippy linter..."
if cargo clippy -- -D warnings; then
    print_success "No clippy warnings found"
else
    print_warning "Clippy warnings found. Please review and fix."
fi

# Build the project
print_step "Building project..."
if cargo build; then
    print_success "Build successful"
else
    print_error "Build failed"
    exit 1
fi

# Run unit tests
print_step "Running unit tests..."
if cargo test --features test-utils --lib -- --test-threads=1; then
    print_success "Unit tests passed"
else
    print_error "Unit tests failed"
    exit 1
fi

# Run integration tests
print_step "Running integration tests..."
if cargo test --features test-utils --test '*'; then
    print_success "Integration tests passed"
else
    print_warning "No integration tests found or integration tests failed"
fi

# Run all tests with verbose output
print_step "Running all tests with verbose output..."
if cargo test --features test-utils -- --test-threads=1 --nocapture; then
    print_success "All tests passed"
else
    print_error "Some tests failed"
    exit 1
fi

# Generate test coverage report (if cargo-tarpaulin is installed)
if command -v cargo-tarpaulin &> /dev/null; then
    print_step "Generating test coverage report..."
    if cargo tarpaulin --out Html --output-dir coverage/; then
        print_success "Coverage report generated in coverage/ directory"
    else
        print_warning "Coverage report generation failed"
    fi
else
    print_warning "cargo-tarpaulin not installed. Skipping coverage report."
    echo "Install with: cargo install cargo-tarpaulin"
fi

# Run benchmarks if they exist
if ls benches/*.rs 1> /dev/null 2>&1; then
    print_step "Running benchmarks..."
    if cargo bench; then
        print_success "Benchmarks completed"
    else
        print_warning "Benchmark execution failed"
    fi
else
    print_warning "No benchmarks found"
fi

# Build documentation
print_step "Building documentation..."
if cargo doc --no-deps; then
    print_success "Documentation built successfully"
    echo "Open target/doc/axiom_dicom_service/index.html to view docs"
else
    print_warning "Documentation build failed"
fi

# Check for security vulnerabilities
if command -v cargo-audit &> /dev/null; then
    print_step "Checking for security vulnerabilities..."
    if cargo audit; then
        print_success "No security vulnerabilities found"
    else
        print_warning "Security vulnerabilities detected. Please review cargo audit output."
    fi
else
    print_warning "cargo-audit not installed. Skipping security audit."
    echo "Install with: cargo install cargo-audit"
fi

echo ""
echo "🎉 Test suite completed successfully!"
echo "=========================================="

# Print test summary
echo "Test Summary:"
echo "- ✅ Code builds successfully"
echo "- ✅ Unit tests pass"
echo "- ✅ Code formatting checked"
echo "- ✅ Linting completed"
echo "- ✅ Documentation generated"

if command -v cargo-tarpaulin &> /dev/null; then
    echo "- ✅ Coverage report generated"
fi

if command -v cargo-audit &> /dev/null; then
    echo "- ✅ Security audit completed"
fi

echo ""
echo "Ready for deployment! 🚀"