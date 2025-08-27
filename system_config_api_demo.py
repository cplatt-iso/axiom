#!/usr/bin/env python3
"""
Test script demonstrating the System Configuration API endpoints.

This script shows how the new API endpoints can be used to manage
system configuration at runtime without editing files.
"""

# Example API calls (using curl or similar HTTP client)

EXAMPLE_REQUESTS = {
    "get_categories": {
        "method": "GET",
        "url": "/api/v1/system-config/categories",
        "description": "Get all configuration categories"
    },
    
    "get_all_config": {
        "method": "GET", 
        "url": "/api/v1/system-config/",
        "description": "Get all configuration settings"
    },
    
    "get_category_config": {
        "method": "GET",
        "url": "/api/v1/system-config/?category=processing",
        "description": "Get configuration settings for specific category"
    },
    
    "update_single_setting": {
        "method": "PUT",
        "url": "/api/v1/system-config/DELETE_ON_SUCCESS",
        "body": {"value": True},
        "description": "Update a single configuration setting"
    },
    
    "bulk_update": {
        "method": "POST",
        "url": "/api/v1/system-config/bulk-update",
        "body": {
            "settings": {
                "DELETE_ON_SUCCESS": False,
                "DUSTBIN_RETENTION_DAYS": 45,
                "CELERY_WORKER_CONCURRENCY": 12
            },
            "ignore_errors": False
        },
        "description": "Update multiple configuration settings at once"
    },
    
    "reset_setting": {
        "method": "DELETE",
        "url": "/api/v1/system-config/DELETE_ON_SUCCESS", 
        "description": "Reset a setting to its default value"
    },
    
    "reload_config": {
        "method": "POST",
        "url": "/api/v1/system-config/reload",
        "description": "Trigger configuration reload"
    }
}

# Example curl commands for testing
CURL_EXAMPLES = [
    "# Get all categories",
    "curl -H 'Authorization: Bearer YOUR_TOKEN' \\",
    "     http://localhost:8000/api/v1/system-config/categories",
    "",
    
    "# Get all configuration settings",
    "curl -H 'Authorization: Bearer YOUR_TOKEN' \\", 
    "     http://localhost:8000/api/v1/system-config/",
    "",
    
    "# Update a single setting",
    "curl -X PUT \\",
    "     -H 'Authorization: Bearer YOUR_TOKEN' \\",
    "     -H 'Content-Type: application/json' \\",
    "     -d '{\"value\": false}' \\",
    "     http://localhost:8000/api/v1/system-config/DELETE_ON_SUCCESS",
    "",
    
    "# Bulk update multiple settings",
    "curl -X POST \\",
    "     -H 'Authorization: Bearer YOUR_TOKEN' \\", 
    "     -H 'Content-Type: application/json' \\",
    "     -d '{\"settings\": {\"DUSTBIN_RETENTION_DAYS\": 45, \"CELERY_WORKER_CONCURRENCY\": 12}, \"ignore_errors\": false}' \\",
    "     http://localhost:8000/api/v1/system-config/bulk-update",
    "",
    
    "# Reset a setting to default",
    "curl -X DELETE \\",
    "     -H 'Authorization: Bearer YOUR_TOKEN' \\",
    "     http://localhost:8000/api/v1/system-config/DELETE_ON_SUCCESS",
]

# Available configuration categories and settings
CONFIGURATION_CATEGORIES = {
    "processing": [
        "DELETE_ON_SUCCESS",
        "DELETE_UNMATCHED_FILES", 
        "DELETE_ON_NO_DESTINATION",
        "MOVE_TO_ERROR_ON_PARTIAL_FAILURE",
        "LOG_ORIGINAL_ATTRIBUTES"
    ],
    "dustbin": [
        "USE_DUSTBIN_SYSTEM",
        "DUSTBIN_RETENTION_DAYS",
        "DUSTBIN_VERIFICATION_TIMEOUT_HOURS"
    ],
    "batch_processing": [
        "EXAM_BATCH_COMPLETION_TIMEOUT",
        "EXAM_BATCH_CHECK_INTERVAL", 
        "EXAM_BATCH_MAX_CONCURRENT"
    ],
    "cleanup": [
        "STALE_DATA_CLEANUP_AGE_DAYS",
        "CLEANUP_BATCH_SIZE"
    ],
    "celery": [
        "CELERY_WORKER_CONCURRENCY",
        "CELERY_PREFETCH_MULTIPLIER",
        "CELERY_TASK_MAX_RETRIES"
    ],
    "dicomweb": [
        "DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS",
        "DICOMWEB_POLLER_QIDO_LIMIT",
        "DICOMWEB_POLLER_MAX_SOURCES"
    ],
    "ai": [
        "AI_VOCAB_CACHE_ENABLED",
        "AI_VOCAB_CACHE_TTL_SECONDS", 
        "VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB"
    ]
}

if __name__ == "__main__":
    print("=== System Configuration API Test Guide ===\n")
    
    print("Configuration Categories:")
    for category, settings in CONFIGURATION_CATEGORIES.items():
        print(f"  {category}:")
        for setting in settings:
            print(f"    - {setting}")
    
    print(f"\nTotal: {sum(len(s) for s in CONFIGURATION_CATEGORIES.values())} modifiable settings")
    print(f"Categories: {len(CONFIGURATION_CATEGORIES)}")
    
    print("\n=== Example curl commands ===")
    for line in CURL_EXAMPLES:
        print(line)
    
    print("\n=== Key Features ===")
    print("✓ Runtime configuration changes without file editing")
    print("✓ Admin role protection for all endpoints")
    print("✓ Type validation and range checking")
    print("✓ Database persistence with fallback to defaults")
    print("✓ Bulk update support")
    print("✓ Reset to defaults functionality")
    print("✓ Categorized settings organization")
    print("✓ Configuration reload triggering")
