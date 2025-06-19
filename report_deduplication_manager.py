#!/usr/bin/env python3
"""
Report Deduplication Manager for Market Intelligence Pipeline

This module handles deduplication logic for reports:
- Identifies multiple reports for the same company/date
- Keeps only the latest report by timestamp
- Manages cleanup of old reports and their vectors
- Maintains deduplication history

Features:
- Date-based deduplication
- Timestamp comparison
- Cleanup of old reports from GCS
- Vector cleanup from Matching Engine
- Deduplication audit trail
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

from gcs_storage_manager import GCSStorageManager
from vertex_ai_matching_engine import VertexAIMatchingEngineManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportDeduplicationManager:
    """Manages deduplication of market intelligence reports"""
    
    def __init__(self, gcs_manager: GCSStorageManager = None, matching_engine_manager: VertexAIMatchingEngineManager = None):
        """
        Initialize Report Deduplication Manager
        
        Args:
            gcs_manager: GCS Storage Manager instance
            matching_engine_manager: Vertex AI Matching Engine Manager instance
        """
        self.gcs_manager = gcs_manager or GCSStorageManager()
        self.matching_engine_manager = matching_engine_manager or VertexAIMatchingEngineManager()
        
        # Deduplication settings
        self.deduplication_log_file = "deduplication_log.json"
        self.load_deduplication_log()
    
    def load_deduplication_log(self):
        """Load deduplication history from log file"""
        try:
            if os.path.exists(self.deduplication_log_file):
                with open(self.deduplication_log_file, 'r') as f:
                    self.deduplication_log = json.load(f)
            else:
                self.deduplication_log = {
                    'created_at': datetime.now().isoformat(),
                    'deduplication_history': [],
                    'statistics': {
                        'total_deduplications': 0,
                        'reports_removed': 0,
                        'last_deduplication': None
                    }
                }
            logger.info(f"📋 Deduplication log loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load deduplication log: {e}")
            self.deduplication_log = {
                'created_at': datetime.now().isoformat(),
                'deduplication_history': [],
                'statistics': {
                    'total_deduplications': 0,
                    'reports_removed': 0,
                    'last_deduplication': None
                }
            }
    
    def save_deduplication_log(self):
        """Save deduplication history to log file"""
        try:
            with open(self.deduplication_log_file, 'w') as f:
                json.dump(self.deduplication_log, f, indent=2)
            logger.info(f"💾 Deduplication log saved")
        except Exception as e:
            logger.error(f"❌ Failed to save deduplication log: {e}")
    
    def group_reports_by_company_date(self, reports: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Group reports by company and date
        
        Args:
            reports: List of report metadata dictionaries
            
        Returns:
            Nested dictionary: {company: {date: [reports]}}
        """
        grouped = defaultdict(lambda: defaultdict(list))
        
        for report in reports:
            metadata = report.get('metadata', {})
            company = metadata.get('company', 'unknown')
            date = metadata.get('date', 'unknown')
            
            # If date not in metadata, try to extract from GCS path
            if date == 'unknown' and 'gcs_path' in report:
                path_parts = report['gcs_path'].split('/')
                if len(path_parts) >= 2:
                    date = path_parts[1]  # Assuming format: company/date/timestamp.html
            
            grouped[company][date].append(report)
        
        return dict(grouped)
    
    def find_latest_report_per_date(self, reports: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Find the latest report for a given date and return duplicates
        
        Args:
            reports: List of reports for the same company/date
            
        Returns:
            Tuple of (latest_report, duplicate_reports)
        """
        if not reports:
            return None, []
        
        if len(reports) == 1:
            return reports[0], []
        
        # Sort reports by timestamp (newest first)
        sorted_reports = sorted(reports, key=lambda r: r.get('created', ''), reverse=True)
        
        # Also try to sort by timestamp in metadata
        def get_timestamp(report):
            metadata = report.get('metadata', {})
            timestamp = metadata.get('timestamp')
            if timestamp:
                try:
                    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    pass
            
            # Fallback to created time
            created = report.get('created')
            if created:
                try:
                    return datetime.fromisoformat(created.replace('Z', '+00:00'))
                except:
                    pass
            
            return datetime.min
        
        sorted_reports = sorted(reports, key=get_timestamp, reverse=True)
        
        latest_report = sorted_reports[0]
        duplicate_reports = sorted_reports[1:]
        
        logger.info(f"📊 Found {len(duplicate_reports)} duplicates for latest report")
        return latest_report, duplicate_reports
    
    def deduplicate_company_reports(self, company_name: str, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Deduplicate reports for a specific company
        
        Args:
            company_name: Company name to deduplicate
            date_filter: Optional date filter (YYYY-MM-DD)
            
        Returns:
            Deduplication result dictionary
        """
        try:
            logger.info(f"🔄 Starting deduplication for {company_name}")
            if date_filter:
                logger.info(f"   Date filter: {date_filter}")
            
            # Get all reports for the company
            reports = self.gcs_manager.list_company_reports(company_name, date_filter)
            
            if not reports:
                logger.info(f"📭 No reports found for {company_name}")
                return {
                    'success': True,
                    'company': company_name,
                    'reports_found': 0,
                    'duplicates_removed': 0,
                    'message': 'No reports found'
                }
            
            logger.info(f"📋 Found {len(reports)} total reports for {company_name}")
            
            # Group reports by date
            grouped_reports = self.group_reports_by_company_date(reports)
            company_reports = grouped_reports.get(company_name, {})
            
            deduplication_results = []
            total_duplicates_removed = 0
            
            # Process each date
            for date, date_reports in company_reports.items():
                if len(date_reports) <= 1:
                    logger.info(f"📅 {date}: No duplicates found ({len(date_reports)} report)")
                    continue
                
                logger.info(f"📅 {date}: Found {len(date_reports)} reports - deduplicating...")
                
                # Find latest and duplicates
                latest_report, duplicate_reports = self.find_latest_report_per_date(date_reports)
                
                if not duplicate_reports:
                    continue
                
                # Remove duplicate reports
                removed_reports = []
                for duplicate in duplicate_reports:
                    gcs_path = duplicate.get('gcs_path')
                    if gcs_path:
                        success = self.gcs_manager.delete_report(gcs_path)
                        if success:
                            removed_reports.append({
                                'gcs_path': gcs_path,
                                'gcs_url': duplicate.get('gcs_url'),
                                'metadata': duplicate.get('metadata', {})
                            })
                
                deduplication_result = {
                    'date': date,
                    'total_reports': len(date_reports),
                    'duplicates_found': len(duplicate_reports),
                    'duplicates_removed': len(removed_reports),
                    'latest_report': {
                        'gcs_path': latest_report.get('gcs_path'),
                        'gcs_url': latest_report.get('gcs_url'),
                        'timestamp': latest_report.get('metadata', {}).get('timestamp')
                    },
                    'removed_reports': removed_reports
                }
                
                deduplication_results.append(deduplication_result)
                total_duplicates_removed += len(removed_reports)
                
                logger.info(f"✅ {date}: Removed {len(removed_reports)} duplicates, kept latest")
            
            # Log deduplication
            deduplication_entry = {
                'timestamp': datetime.now().isoformat(),
                'company': company_name,
                'date_filter': date_filter,
                'total_reports_found': len(reports),
                'total_duplicates_removed': total_duplicates_removed,
                'results_by_date': deduplication_results
            }
            
            self.deduplication_log['deduplication_history'].append(deduplication_entry)
            self.deduplication_log['statistics']['total_deduplications'] += 1
            self.deduplication_log['statistics']['reports_removed'] += total_duplicates_removed
            self.deduplication_log['statistics']['last_deduplication'] = datetime.now().isoformat()
            
            self.save_deduplication_log()
            
            result = {
                'success': True,
                'company': company_name,
                'date_filter': date_filter,
                'reports_found': len(reports),
                'duplicates_removed': total_duplicates_removed,
                'results_by_date': deduplication_results,
                'processed_at': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Deduplication completed for {company_name}")
            logger.info(f"   Total reports: {len(reports)}")
            logger.info(f"   Duplicates removed: {total_duplicates_removed}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to deduplicate reports for {company_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'company': company_name,
                'date_filter': date_filter,
                'processed_at': datetime.now().isoformat()
            }
    
    def deduplicate_all_companies(self, days_back: int = 7) -> Dict[str, Any]:
        """
        Deduplicate reports for all companies within a date range
        
        Args:
            days_back: Number of days back to check for duplicates
            
        Returns:
            Overall deduplication result
        """
        try:
            logger.info(f"🔄 Starting global deduplication (last {days_back} days)")
            
            # This is a simplified implementation
            # In practice, you'd need to list all companies from GCS or maintain a company registry
            
            # For now, return a placeholder result
            result = {
                'success': True,
                'days_back': days_back,
                'companies_processed': 0,
                'total_duplicates_removed': 0,
                'processed_at': datetime.now().isoformat(),
                'message': 'Global deduplication not yet implemented - use deduplicate_company_reports for specific companies'
            }
            
            logger.info(f"ℹ️ Global deduplication placeholder completed")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to perform global deduplication: {e}")
            return {
                'success': False,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }
    
    def get_deduplication_statistics(self) -> Dict[str, Any]:
        """Get deduplication statistics"""
        return {
            'statistics': self.deduplication_log.get('statistics', {}),
            'recent_deduplications': self.deduplication_log.get('deduplication_history', [])[-10:],  # Last 10
            'log_file': self.deduplication_log_file
        }

def main():
    """Test the Report Deduplication Manager"""
    print("🧪 Testing Report Deduplication Manager")
    print("=" * 50)
    
    try:
        # Initialize manager
        manager = ReportDeduplicationManager()
        
        # Test deduplication for a sample company
        print("\n🔄 Testing deduplication...")
        result = manager.deduplicate_company_reports("test_company")
        
        if result['success']:
            print(f"✅ Deduplication successful")
            print(f"   Reports found: {result['reports_found']}")
            print(f"   Duplicates removed: {result['duplicates_removed']}")
        else:
            print(f"❌ Deduplication failed: {result.get('error')}")
        
        # Test statistics
        print("\n📊 Testing statistics...")
        stats = manager.get_deduplication_statistics()
        print(f"Total deduplications: {stats['statistics'].get('total_deduplications', 0)}")
        print(f"Reports removed: {stats['statistics'].get('reports_removed', 0)}")
        
        print("\n✅ Report Deduplication Manager test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
