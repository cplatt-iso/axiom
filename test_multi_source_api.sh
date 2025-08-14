#!/bin/bash

# Test script for multi-source spanner API endpoints
API_BASE="https://axiom.trazen.org/api/v1"
API_KEY="9HXqH14f_IA59fdhJdDNpdLesTXz2CRVWCPfojxGs"

echo "Testing multi-source spanner API endpoints..."
echo "============================================="

# Test 1: Get existing spanner config
echo -e "\n1. Getting spanner config 2..."
curl -s -H "Api-Key: ${API_KEY}" "${API_BASE}/config/spanner/2" | jq '.'

# Test 2: Get existing source mappings for spanner 2
echo -e "\n2. Getting existing source mappings for spanner 2..."
curl -s -H "Api-Key: ${API_KEY}" "${API_BASE}/config/spanner/2/sources" | jq '.'

# Test 3: Try to add DICOMweb source (this should work now)
echo -e "\n3. Testing DICOMweb source addition..."
curl -s -X POST \
  -H "Api-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "priority": 50,
    "is_enabled": true,
    "weight": 1,
    "enable_failover": true,
    "max_retries": 2,
    "retry_delay_seconds": 5,
    "source_type": "dicomweb",
    "dimse_qr_source_id": null,
    "dicomweb_source_id": 1,
    "google_healthcare_source_id": null
  }' \
  "${API_BASE}/config/spanner/2/sources"

# Test 4: Try to add Google Healthcare source
echo -e "\n4. Testing Google Healthcare source addition..."
curl -s -X POST \
  -H "Api-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "priority": 60,
    "is_enabled": true,
    "weight": 1,
    "enable_failover": true,
    "max_retries": 2,
    "retry_delay_seconds": 5,
    "source_type": "google_healthcare",
    "dimse_qr_source_id": null,
    "dicomweb_source_id": null,
    "google_healthcare_source_id": 1
  }' \
  "${API_BASE}/config/spanner/2/sources"

# Test 5: Try to add DIMSE source (should still work)
echo -e "\n5. Testing DIMSE source addition..."
curl -s -X POST \
  -H "Api-Key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "priority": 70,
    "is_enabled": true,
    "weight": 1,
    "enable_failover": true,
    "max_retries": 2,
    "retry_delay_seconds": 5,
    "source_type": "dimse-qr",
    "dimse_qr_source_id": 1,
    "dicomweb_source_id": null,
    "google_healthcare_source_id": null
  }' \
  "${API_BASE}/config/spanner/2/sources"

echo -e "\n============================================="
echo "API testing complete!"
