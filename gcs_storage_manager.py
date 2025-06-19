#!/usr/bin/env python3
"""
Google Cloud Storage Manager for Market Intelligence Pipeline

This module handles uploading HTML reports to GCS with structured paths:
gs://bucket/{company}/{YYYY-MM-DD}/{HH-MM-SS}.html

Features:
- Structured path generation
- Metadata attachment
- Duplicate handling
- Error handling and retries
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

try:
    from google.cloud import storage
    from google.cloud.exceptions import GoogleCloudError
except ImportError:
    print("❌ Google Cloud Storage library not installed.")
    print("Install with: pip install google-cloud-storage")
    raise

from config import GCS_CREDENTIALS_PATH
from gcp_project_config import GCS_BUCKET_NAME, GCP_PROJECT_ID, get_gcs_report_path, get_gcs_full_url

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GCSStorageManager:
    """Manages Google Cloud Storage operations for market intelligence reports"""
    
    def __init__(self, credentials_path: str = None):
        """
        Initialize GCS Storage Manager
        
        Args:
            credentials_path: Path to GCS service account credentials JSON file
        """
        self.credentials_path = credentials_path or GCS_CREDENTIALS_PATH
        self.bucket_name = GCS_BUCKET_NAME
        self.project_id = GCP_PROJECT_ID
        self.client = None
        self.bucket = None
        
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize GCS client with service account credentials"""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"GCS credentials not found: {self.credentials_path}")
            
            # Initialize client with service account credentials
            self.client = storage.Client.from_service_account_json(
                self.credentials_path,
                project=self.project_id
            )
            
            # Get bucket reference
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Test bucket access
            if not self.bucket.exists():
                raise ValueError(f"GCS bucket does not exist: {self.bucket_name}")
            
            logger.info(f"✅ GCS client initialized successfully")
            logger.info(f"   Project: {self.project_id}")
            logger.info(f"   Bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize GCS client: {e}")
            raise
    
    def upload_report(
        self, 
        html_file_path: str, 
        company_name: str,
        custom_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Upload HTML report to GCS with structured path
        
        Args:
            html_file_path: Local path to HTML report file
            company_name: Company name for the report
            custom_timestamp: Optional custom timestamp (defaults to now)
            
        Returns:
            Dict containing upload results and metadata
        """
        try:
            # Validate input file
            if not os.path.exists(html_file_path):
                raise FileNotFoundError(f"HTML report file not found: {html_file_path}")
            
            # Generate timestamp and paths
            timestamp = custom_timestamp or datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")
            time_str = timestamp.strftime("%H-%M-%S")
            
            # Generate GCS path
            gcs_path = get_gcs_report_path(company_name, date_str, time_str)
            gcs_url = get_gcs_full_url(company_name, date_str, time_str)
            
            # Read HTML content
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Create blob and upload
            blob = self.bucket.blob(gcs_path)
            
            # Set metadata
            metadata = {
                'company': company_name,
                'date': date_str,
                'timestamp': timestamp.isoformat(),
                'format': 'html',
                'source': 'market_intelligence_pipeline',
                'file_size': str(len(html_content.encode('utf-8')))
            }
            blob.metadata = metadata
            
            # Set content type
            blob.content_type = 'text/html'
            
            # Upload content
            blob.upload_from_string(html_content, content_type='text/html')
            
            logger.info(f"✅ Report uploaded successfully")
            logger.info(f"   Company: {company_name}")
            logger.info(f"   GCS Path: {gcs_path}")
            logger.info(f"   Size: {len(html_content)} characters")
            
            return {
                'success': True,
                'gcs_url': gcs_url,
                'gcs_path': gcs_path,
                'company': company_name,
                'date': date_str,
                'timestamp': timestamp.isoformat(),
                'metadata': metadata,
                'file_size': len(html_content)
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to upload report: {e}")
            return {
                'success': False,
                'error': str(e),
                'company': company_name,
                'file_path': html_file_path
            }
    
    def list_company_reports(self, company_name: str, date_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all reports for a company, optionally filtered by date
        
        Args:
            company_name: Company name to filter by
            date_filter: Optional date filter (YYYY-MM-DD format)
            
        Returns:
            List of report metadata dictionaries
        """
        try:
            company_clean = company_name.replace(' ', '_').replace('.', '').lower()
            prefix = f"{company_clean}/"
            
            if date_filter:
                prefix = f"{company_clean}/{date_filter}/"
            
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            
            reports = []
            for blob in blobs:
                if blob.name.endswith('.html'):
                    report_info = {
                        'gcs_path': blob.name,
                        'gcs_url': f"gs://{self.bucket_name}/{blob.name}",
                        'size': blob.size,
                        'created': blob.time_created.isoformat() if blob.time_created else None,
                        'updated': blob.updated.isoformat() if blob.updated else None,
                        'metadata': blob.metadata or {}
                    }
                    reports.append(report_info)
            
            # Sort by creation time (newest first)
            reports.sort(key=lambda x: x['created'] or '', reverse=True)
            
            logger.info(f"📋 Found {len(reports)} reports for {company_name}")
            return reports
            
        except Exception as e:
            logger.error(f"❌ Failed to list reports: {e}")
            return []
    
    def get_latest_report_for_date(self, company_name: str, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest report for a specific company and date
        
        Args:
            company_name: Company name
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            Latest report metadata or None if not found
        """
        reports = self.list_company_reports(company_name, date_str)
        return reports[0] if reports else None
    
    def download_report(self, gcs_path: str, local_path: str) -> bool:
        """
        Download a report from GCS to local file
        
        Args:
            gcs_path: GCS path of the report
            local_path: Local path to save the report
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            
            if not blob.exists():
                logger.error(f"❌ Report not found in GCS: {gcs_path}")
                return False
            
            # Create directory if needed
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download file
            blob.download_to_filename(local_path)
            
            logger.info(f"✅ Report downloaded: {gcs_path} -> {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to download report: {e}")
            return False
    
    def delete_report(self, gcs_path: str) -> bool:
        """
        Delete a report from GCS
        
        Args:
            gcs_path: GCS path of the report to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            
            if not blob.exists():
                logger.warning(f"⚠️ Report not found for deletion: {gcs_path}")
                return True  # Already deleted
            
            blob.delete()
            logger.info(f"🗑️ Report deleted: {gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete report: {e}")
            return False

def main():
    """Test the GCS Storage Manager"""
    print("🧪 Testing GCS Storage Manager")
    print("=" * 50)
    
    try:
        # Initialize manager
        manager = GCSStorageManager()
        
        # Test listing (should work even without uploads)
        print("\n📋 Testing report listing...")
        reports = manager.list_company_reports("test_company")
        print(f"Found {len(reports)} existing reports")
        
        print("\n✅ GCS Storage Manager test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
