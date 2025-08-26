#!/usr/bin/env python3
"""
Test script to verify enhanced logging configuration
"""
import sys
import os
sys.path.insert(0, '/home/icculus/axiom/backend')

from app.core.logging_config import configure_json_logging
import structlog

def test_enhanced_logging():
    """Test that enhanced parameters appear in JSON logs"""
    
    # Setup logging
    configure_json_logging(service_name="test-service", log_level="INFO")
    
    # Get a logger
    logger = structlog.get_logger()
    
    print("=== Testing Basic Logging ===")
    logger.info("Basic log message")
    
    print("\n=== Testing Enhanced Logging with Context ===")
    logger.info(
        "DICOM transmission started",
        destination_ae_title="TEST_DEST",
        file_count=5,
        verification_id="test-123",
        total_size_mb=125.5,
        connection_type="C-STORE"
    )
    
    print("\n=== Testing Bound Logger Context ===")
    bound_logger = logger.bind(
        service="dcm4che-sender",
        destination="PACS_SERVER"
    )
    
    bound_logger.info(
        "Files sent successfully", 
        files_processed=3,
        success_rate=100.0,
        transfer_time_seconds=45.2
    )
    
    print("\n=== Testing Error Logging ===")
    logger.error(
        "Transmission failed",
        error_code="C-STORE-001",
        ae_title="FAILED_DEST",
        retry_count=3,
        last_error="Connection timeout"
    )

if __name__ == "__main__":
    test_enhanced_logging()
