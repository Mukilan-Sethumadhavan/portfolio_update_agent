#!/usr/bin/env python3
"""
GCP Credentials Validator

This script validates that all required GCP credentials and configurations
are properly set up for the Market Intelligence Pipeline.
"""

import os
import json
import sys
from pathlib import Path

def check_file_exists(filepath: str, description: str) -> bool:
    """Check if a file exists and is readable"""
    if not os.path.exists(filepath):
        print(f"❌ {description} not found: {filepath}")
        return False
    
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            if not content.strip():
                print(f"❌ {description} is empty: {filepath}")
                return False
        print(f"✅ {description} found: {filepath}")
        return True
    except Exception as e:
        print(f"❌ Error reading {description}: {e}")
        return False

def validate_json_credentials(filepath: str, description: str) -> bool:
    """Validate JSON credential file structure"""
    try:
        with open(filepath, 'r') as f:
            creds = json.load(f)
        
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
        missing_fields = [field for field in required_fields if field not in creds]
        
        if missing_fields:
            print(f"❌ {description} missing required fields: {missing_fields}")
            return False
        
        if creds.get('type') != 'service_account':
            print(f"❌ {description} is not a service account credential")
            return False
            
        print(f"✅ {description} is valid JSON service account credential")
        print(f"   Project ID: {creds.get('project_id')}")
        print(f"   Service Account: {creds.get('client_email')}")
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ {description} is not valid JSON: {e}")
        return False
    except Exception as e:
        print(f"❌ Error validating {description}: {e}")
        return False

def validate_gcp_configuration():
    """Validate GCP project configuration"""
    try:
        from gcp_project_config import (
            GCP_PROJECT_ID, 
            GCS_BUCKET_NAME,
            MATCHING_ENGINE_INDEX_ENDPOINT,
            MATCHING_ENGINE_INDEX_ID,
            MATCHING_ENGINE_DEPLOYED_INDEX_ID,
            validate_configuration
        )
        
        print("\n📋 GCP Project Configuration:")
        print(f"   Project ID: {GCP_PROJECT_ID}")
        print(f"   GCS Bucket: {GCS_BUCKET_NAME}")
        print(f"   Index Endpoint: {MATCHING_ENGINE_INDEX_ENDPOINT or 'Not set'}")
        print(f"   Index ID: {MATCHING_ENGINE_INDEX_ID or 'Not set'}")
        print(f"   Deployed Index ID: {MATCHING_ENGINE_DEPLOYED_INDEX_ID}")
        
        return validate_configuration()
        
    except ImportError as e:
        print(f"❌ Cannot import gcp_project_config.py: {e}")
        return False
    except Exception as e:
        print(f"❌ Error validating GCP configuration: {e}")
        return False

def test_gcs_credentials():
    """Test GCS credentials by attempting to initialize client"""
    try:
        from google.cloud import storage
        
        # Try to initialize client with credentials
        client = storage.Client.from_service_account_json('gcp_storage_credentials.json')
        
        # Try to list buckets (basic permission test)
        buckets = list(client.list_buckets())
        print(f"✅ GCS credentials work - can access {len(buckets)} buckets")
        return True
        
    except Exception as e:
        print(f"❌ GCS credentials test failed: {e}")
        print("   Make sure you have installed: pip install google-cloud-storage")
        return False

def test_vertex_ai_credentials():
    """Test Vertex AI credentials by attempting to initialize client"""
    try:
        from google.cloud import aiplatform
        
        # Try to initialize with credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp_vertex_ai_credentials.json'
        
        # Basic initialization test
        aiplatform.init(project='test-project', location='us-central1')
        print("✅ Vertex AI credentials appear to be valid")
        return True
        
    except Exception as e:
        print(f"❌ Vertex AI credentials test failed: {e}")
        print("   Make sure you have installed: pip install google-cloud-aiplatform")
        return False

def main():
    """Main validation function"""
    print("🔍 GCP Credentials Validation")
    print("=" * 50)
    
    all_valid = True
    
    # Check credential files exist
    print("\n📁 Checking credential files...")
    storage_creds_valid = check_file_exists('gcp_storage_credentials.json', 'GCS credentials')
    vertex_creds_valid = check_file_exists('gcp_vertex_ai_credentials.json', 'Vertex AI credentials')
    config_valid = check_file_exists('gcp_project_config.py', 'GCP project configuration')
    
    all_valid = all_valid and storage_creds_valid and vertex_creds_valid and config_valid
    
    # Validate JSON structure
    if storage_creds_valid:
        print("\n🔍 Validating GCS credentials structure...")
        storage_json_valid = validate_json_credentials('gcp_storage_credentials.json', 'GCS credentials')
        all_valid = all_valid and storage_json_valid
    
    if vertex_creds_valid:
        print("\n🔍 Validating Vertex AI credentials structure...")
        vertex_json_valid = validate_json_credentials('gcp_vertex_ai_credentials.json', 'Vertex AI credentials')
        all_valid = all_valid and vertex_json_valid
    
    # Validate configuration
    if config_valid:
        print("\n🔍 Validating GCP project configuration...")
        config_validation = validate_gcp_configuration()
        all_valid = all_valid and config_validation
    
    # Test actual credentials (optional - requires packages)
    print("\n🧪 Testing credentials (optional)...")
    if storage_creds_valid:
        test_gcs_credentials()
    
    if vertex_creds_valid:
        test_vertex_ai_credentials()
    
    # Final result
    print("\n" + "=" * 50)
    if all_valid:
        print("🎉 All validations passed! Your GCP setup is ready.")
    else:
        print("❌ Some validations failed. Please check the errors above.")
        print("📖 See gcp_credentials_setup.md for detailed setup instructions.")
    
    print("=" * 50)
    return all_valid

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
