#!/usr/bin/env python3
"""
Test script to verify Elasticsearch TLS certificate configuration.
Run this to debug certificate verification issues.
"""
import os
import sys
import ssl
import socket
from pathlib import Path

# Add the app to the Python path
sys.path.insert(0, '/home/icculus/axiom/backend')

from app.core.config import settings


def test_certificate_file():
    """Test if the CA certificate file exists and is readable."""
    print("🔐 Testing Certificate File Setup")
    print("=" * 40)
    
    ca_cert_path = getattr(settings, 'ELASTICSEARCH_CA_CERT_PATH', None)
    
    if not ca_cert_path:
        print("❌ No CA certificate path configured")
        return False
    
    print(f"📁 Certificate path: {ca_cert_path}")
    
    if not os.path.exists(ca_cert_path):
        print(f"❌ Certificate file does not exist: {ca_cert_path}")
        return False
    
    try:
        with open(ca_cert_path, 'r') as f:
            cert_content = f.read()
            if "BEGIN CERTIFICATE" in cert_content:
                print("✅ Certificate file exists and appears valid")
                print(f"📊 Certificate size: {len(cert_content)} bytes")
                return True
            else:
                print("❌ Certificate file exists but doesn't appear to be a valid certificate")
                return False
    except Exception as e:
        print(f"❌ Cannot read certificate file: {e}")
        return False


def test_ssl_connection():
    """Test SSL connection to Elasticsearch."""
    print("\n🔗 Testing SSL Connection")
    print("=" * 40)
    
    host = getattr(settings, 'ELASTICSEARCH_HOST', 'localhost')
    port = getattr(settings, 'ELASTICSEARCH_PORT', 9200)
    
    try:
        # Create SSL context
        context = ssl.create_default_context()
        
        ca_cert_path = getattr(settings, 'ELASTICSEARCH_CA_CERT_PATH', None)
        verify_certs = getattr(settings, 'ELASTICSEARCH_VERIFY_CERTS', False)
        
        if verify_certs and ca_cert_path and os.path.exists(ca_cert_path):
            context.load_verify_locations(ca_cert_path)
            print(f"📋 Loaded CA certificate: {ca_cert_path}")
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            print("⚠️  Certificate verification disabled")
        
        # Test connection
        print(f"🔌 Connecting to {host}:{port}...")
        
        with socket.create_connection((host, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=host) as ssock:
                print(f"✅ SSL connection successful")
                print(f"📜 SSL version: {ssock.version()}")
                print(f"🔐 Cipher: {ssock.cipher()}")
                
                # Get certificate info
                cert = ssock.getpeercert()
                if cert:
                    print(f"🏷️  Certificate subject: {cert.get('subject', 'Unknown')}")
                    print(f"📅 Certificate expires: {cert.get('notAfter', 'Unknown')}")
                
                return True
                
    except socket.timeout:
        print(f"❌ Connection timeout to {host}:{port}")
        return False
    except ssl.SSLError as e:
        print(f"❌ SSL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False


def test_elasticsearch_client():
    """Test the actual Elasticsearch client configuration."""
    print("\n🔍 Testing Elasticsearch Client")
    print("=" * 40)
    
    try:
        from app.services.log_service import log_service
        
        # Override for local testing if needed
        if getattr(settings, 'ELASTICSEARCH_HOST', '') == 'elasticsearch':
            print("🔧 Overriding host to localhost for local testing")
            log_service.es_host = "localhost"
            log_service._initialize_client()
        
        health = log_service.health_check()
        
        print(f"📊 Health check result: {health}")
        
        if health.get("status") == "ok":
            print("✅ Elasticsearch client connection successful")
            
            tls_info = health.get("tls_info", {})
            if tls_info:
                print(f"🔐 TLS Info:")
                print(f"   Scheme: {tls_info.get('scheme', 'unknown')}")
                print(f"   TLS Enabled: {tls_info.get('tls_enabled', False)}")
                print(f"   Cert Verification: {tls_info.get('certificate_verification', False)}")
            
            return True
        else:
            print(f"❌ Elasticsearch connection failed: {health.get('message', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Client test failed: {e}")
        return False


def main():
    print("🚀 Elasticsearch TLS Certificate Verification Test")
    print("=" * 60)
    
    print(f"📋 Configuration:")
    print(f"   Host: {getattr(settings, 'ELASTICSEARCH_HOST', 'not set')}")
    print(f"   Port: {getattr(settings, 'ELASTICSEARCH_PORT', 'not set')}")
    print(f"   Scheme: {getattr(settings, 'ELASTICSEARCH_SCHEME', 'not set')}")
    print(f"   Verify Certs: {getattr(settings, 'ELASTICSEARCH_VERIFY_CERTS', 'not set')}")
    print(f"   CA Cert Path: {getattr(settings, 'ELASTICSEARCH_CA_CERT_PATH', 'not set')}")
    print()
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Certificate file
    if test_certificate_file():
        tests_passed += 1
    
    # Test 2: SSL connection  
    if test_ssl_connection():
        tests_passed += 1
    
    # Test 3: Elasticsearch client
    if test_elasticsearch_client():
        tests_passed += 1
    
    print(f"\n📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! TLS setup is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the configuration above.")
        return 1


if __name__ == "__main__":
    exit(main())
