#!/usr/bin/env python3
"""
Market Intelligence Pipeline Orchestrator

This is the main pipeline that orchestrates the complete flow:
1. Takes HTML report from master_controller.py
2. Uploads to GCS with structured path
3. Generates embeddings using Vertex AI
4. Stores vectors in Matching Engine
5. Handles deduplication
6. Provides status and monitoring

Usage:
    pipeline = MarketIntelligencePipeline()
    result = pipeline.process_report("final_report_tesla.html", "Tesla")
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from gcs_storage_manager import GCSStorageManager
from vertex_ai_embedding_service import VertexAIEmbeddingService
from vertex_ai_matching_engine import VertexAIMatchingEngineManager
from report_deduplication_manager import ReportDeduplicationManager
from gcp_project_config import validate_configuration

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketIntelligencePipeline:
    """Main pipeline orchestrator for market intelligence reports"""
    
    def __init__(self, enable_deduplication: bool = True):
        """
        Initialize Market Intelligence Pipeline
        
        Args:
            enable_deduplication: Whether to enable automatic deduplication
        """
        self.enable_deduplication = enable_deduplication
        self.pipeline_log_file = "pipeline_log.json"
        
        # Initialize components
        self.gcs_manager = None
        self.embedding_service = None
        self.matching_engine_manager = None
        self.deduplication_manager = None
        
        self.load_pipeline_log()
        self._initialize_components()
    
    def load_pipeline_log(self):
        """Load pipeline execution log"""
        try:
            if os.path.exists(self.pipeline_log_file):
                with open(self.pipeline_log_file, 'r') as f:
                    self.pipeline_log = json.load(f)
            else:
                self.pipeline_log = {
                    'created_at': datetime.now().isoformat(),
                    'pipeline_runs': [],
                    'statistics': {
                        'total_reports_processed': 0,
                        'successful_runs': 0,
                        'failed_runs': 0,
                        'last_run': None
                    }
                }
            logger.info(f"📋 Pipeline log loaded")
        except Exception as e:
            logger.error(f"❌ Failed to load pipeline log: {e}")
            self.pipeline_log = {
                'created_at': datetime.now().isoformat(),
                'pipeline_runs': [],
                'statistics': {
                    'total_reports_processed': 0,
                    'successful_runs': 0,
                    'failed_runs': 0,
                    'last_run': None
                }
            }
    
    def save_pipeline_log(self):
        """Save pipeline execution log"""
        try:
            with open(self.pipeline_log_file, 'w') as f:
                json.dump(self.pipeline_log, f, indent=2)
            logger.info(f"💾 Pipeline log saved")
        except Exception as e:
            logger.error(f"❌ Failed to save pipeline log: {e}")
    
    def _initialize_components(self):
        """Initialize all pipeline components"""
        try:
            logger.info(f"🔧 Initializing pipeline components...")
            
            # Validate configuration first
            if not validate_configuration():
                raise ValueError("GCP configuration validation failed")
            
            # Initialize components
            self.gcs_manager = GCSStorageManager()
            self.embedding_service = VertexAIEmbeddingService()
            self.matching_engine_manager = VertexAIMatchingEngineManager()
            
            if self.enable_deduplication:
                self.deduplication_manager = ReportDeduplicationManager(
                    self.gcs_manager, 
                    self.matching_engine_manager
                )
            
            logger.info(f"✅ All pipeline components initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize pipeline components: {e}")
            raise
    
    def process_report(self, html_file_path: str, company_name: str, custom_timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Process a single HTML report through the complete pipeline
        
        Args:
            html_file_path: Path to the HTML report file
            company_name: Company name for the report
            custom_timestamp: Optional custom timestamp
            
        Returns:
            Pipeline execution result
        """
        run_id = f"{company_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        logger.info(f"🚀 Starting pipeline run: {run_id}")
        logger.info(f"   Company: {company_name}")
        logger.info(f"   HTML file: {html_file_path}")
        
        pipeline_result = {
            'run_id': run_id,
            'company': company_name,
            'html_file_path': html_file_path,
            'start_time': start_time.isoformat(),
            'steps': {},
            'success': False
        }
        
        try:
            # Step 1: Upload to GCS
            logger.info(f"📦 Step 1: Uploading report to GCS...")
            gcs_result = self.gcs_manager.upload_report(html_file_path, company_name, custom_timestamp)
            pipeline_result['steps']['gcs_upload'] = gcs_result
            
            if not gcs_result['success']:
                raise Exception(f"GCS upload failed: {gcs_result.get('error')}")
            
            gcs_url = gcs_result['gcs_url']
            logger.info(f"✅ Step 1 completed: {gcs_url}")
            
            # Step 2: Generate embeddings
            logger.info(f"🧠 Step 2: Generating embeddings...")
            embedding_result = self.embedding_service.process_html_file(
                html_file_path, 
                company_name, 
                gcs_result.get('metadata', {})
            )
            pipeline_result['steps']['embedding_generation'] = embedding_result
            
            if not embedding_result['success']:
                raise Exception(f"Embedding generation failed: {embedding_result.get('error')}")
            
            logger.info(f"✅ Step 2 completed: {embedding_result['num_chunks']} chunks embedded")
            
            # Step 3: Store vectors in Matching Engine
            logger.info(f"🔍 Step 3: Storing vectors in Matching Engine...")
            vector_result = self.matching_engine_manager.process_embedding_result(embedding_result, gcs_url)
            pipeline_result['steps']['vector_storage'] = vector_result
            
            if not vector_result['success']:
                raise Exception(f"Vector storage failed: {vector_result.get('error')}")
            
            logger.info(f"✅ Step 3 completed: {vector_result['datapoints_upserted']} vectors stored")
            
            # Step 4: Deduplication (if enabled)
            if self.enable_deduplication:
                logger.info(f"🔄 Step 4: Running deduplication...")
                dedup_result = self.deduplication_manager.deduplicate_company_reports(
                    company_name, 
                    gcs_result.get('date')
                )
                pipeline_result['steps']['deduplication'] = dedup_result
                
                if dedup_result['success']:
                    logger.info(f"✅ Step 4 completed: {dedup_result['duplicates_removed']} duplicates removed")
                else:
                    logger.warning(f"⚠️ Step 4 had issues: {dedup_result.get('error')}")
            else:
                logger.info(f"⏭️ Step 4 skipped: Deduplication disabled")
                pipeline_result['steps']['deduplication'] = {'skipped': True, 'reason': 'Deduplication disabled'}
            
            # Pipeline completed successfully
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_result.update({
                'success': True,
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'final_gcs_url': gcs_url,
                'chunks_processed': embedding_result.get('num_chunks', 0),
                'vectors_stored': vector_result.get('datapoints_upserted', 0)
            })
            
            logger.info(f"🎉 Pipeline run completed successfully: {run_id}")
            logger.info(f"   Duration: {duration:.2f} seconds")
            logger.info(f"   Final GCS URL: {gcs_url}")
            logger.info(f"   Chunks processed: {embedding_result.get('num_chunks', 0)}")
            logger.info(f"   Vectors stored: {vector_result.get('datapoints_upserted', 0)}")
            
            # Update statistics
            self.pipeline_log['statistics']['successful_runs'] += 1
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_result.update({
                'success': False,
                'error': str(e),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration
            })
            
            logger.error(f"❌ Pipeline run failed: {run_id}")
            logger.error(f"   Error: {e}")
            logger.error(f"   Duration: {duration:.2f} seconds")
            
            # Update statistics
            self.pipeline_log['statistics']['failed_runs'] += 1
        
        # Log the run
        self.pipeline_log['pipeline_runs'].append(pipeline_result)
        self.pipeline_log['statistics']['total_reports_processed'] += 1
        self.pipeline_log['statistics']['last_run'] = end_time.isoformat()
        
        # Keep only last 100 runs in log
        if len(self.pipeline_log['pipeline_runs']) > 100:
            self.pipeline_log['pipeline_runs'] = self.pipeline_log['pipeline_runs'][-100:]
        
        self.save_pipeline_log()
        
        return pipeline_result
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Get pipeline execution statistics"""
        return {
            'statistics': self.pipeline_log.get('statistics', {}),
            'recent_runs': self.pipeline_log.get('pipeline_runs', [])[-10:],  # Last 10 runs
            'log_file': self.pipeline_log_file,
            'components_initialized': {
                'gcs_manager': self.gcs_manager is not None,
                'embedding_service': self.embedding_service is not None,
                'matching_engine_manager': self.matching_engine_manager is not None,
                'deduplication_manager': self.deduplication_manager is not None
            }
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on all pipeline components"""
        health_status = {
            'overall_status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        try:
            # Check GCS Manager
            if self.gcs_manager:
                health_status['components']['gcs_manager'] = {'status': 'healthy', 'message': 'Initialized'}
            else:
                health_status['components']['gcs_manager'] = {'status': 'unhealthy', 'message': 'Not initialized'}
            
            # Check Embedding Service
            if self.embedding_service:
                health_status['components']['embedding_service'] = {'status': 'healthy', 'message': 'Initialized'}
            else:
                health_status['components']['embedding_service'] = {'status': 'unhealthy', 'message': 'Not initialized'}
            
            # Check Matching Engine Manager
            if self.matching_engine_manager:
                health_status['components']['matching_engine_manager'] = {'status': 'healthy', 'message': 'Initialized'}
            else:
                health_status['components']['matching_engine_manager'] = {'status': 'unhealthy', 'message': 'Not initialized'}
            
            # Check Deduplication Manager
            if self.enable_deduplication:
                if self.deduplication_manager:
                    health_status['components']['deduplication_manager'] = {'status': 'healthy', 'message': 'Initialized'}
                else:
                    health_status['components']['deduplication_manager'] = {'status': 'unhealthy', 'message': 'Not initialized'}
            else:
                health_status['components']['deduplication_manager'] = {'status': 'disabled', 'message': 'Deduplication disabled'}
            
            # Check overall status
            unhealthy_components = [name for name, status in health_status['components'].items() 
                                 if status['status'] == 'unhealthy']
            
            if unhealthy_components:
                health_status['overall_status'] = 'unhealthy'
                health_status['unhealthy_components'] = unhealthy_components
            
        except Exception as e:
            health_status['overall_status'] = 'error'
            health_status['error'] = str(e)
        
        return health_status

def main():
    """Test the Market Intelligence Pipeline"""
    print("🧪 Testing Market Intelligence Pipeline")
    print("=" * 50)
    
    try:
        # Initialize pipeline
        pipeline = MarketIntelligencePipeline(enable_deduplication=True)
        
        # Health check
        print("\n🏥 Performing health check...")
        health = pipeline.health_check()
        print(f"Overall status: {health['overall_status']}")
        
        for component, status in health['components'].items():
            print(f"   {component}: {status['status']} - {status['message']}")
        
        # Statistics
        print("\n📊 Pipeline statistics...")
        stats = pipeline.get_pipeline_statistics()
        print(f"Total reports processed: {stats['statistics'].get('total_reports_processed', 0)}")
        print(f"Successful runs: {stats['statistics'].get('successful_runs', 0)}")
        print(f"Failed runs: {stats['statistics'].get('failed_runs', 0)}")
        
        print("\n✅ Market Intelligence Pipeline test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
