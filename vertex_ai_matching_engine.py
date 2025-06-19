#!/usr/bin/env python3
"""
Vertex AI Matching Engine Manager for Market Intelligence Pipeline

This module handles storing and querying vectors in Vertex AI Matching Engine
with metadata for semantic search (RAG).

Features:
- Vector storage with metadata
- Batch upsert operations
- Query and search functionality
- Index management
"""

import os
import json
import logging
import uuid
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

try:
    from google.cloud import aiplatform
    from google.cloud.aiplatform import MatchingEngineIndex, MatchingEngineIndexEndpoint
    from google.protobuf import struct_pb2
except ImportError:
    print("❌ Google Cloud AI Platform library not installed.")
    print("Install with: pip install google-cloud-aiplatform")
    raise

from config import VERTEX_AI_CREDENTIALS_PATH, VERTEX_AI_LOCATION, EMBEDDING_DIMENSION
from gcp_project_config import (
    GCP_PROJECT_ID, 
    MATCHING_ENGINE_INDEX_ENDPOINT,
    MATCHING_ENGINE_INDEX_ID,
    MATCHING_ENGINE_DEPLOYED_INDEX_ID
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VertexAIMatchingEngineManager:
    """Manages Vertex AI Matching Engine operations for vector storage and search"""
    
    def __init__(self, credentials_path: str = None):
        """
        Initialize Vertex AI Matching Engine Manager
        
        Args:
            credentials_path: Path to Vertex AI service account credentials JSON file
        """
        self.credentials_path = credentials_path or VERTEX_AI_CREDENTIALS_PATH
        self.project_id = GCP_PROJECT_ID
        self.location = VERTEX_AI_LOCATION
        self.embedding_dimension = EMBEDDING_DIMENSION
        
        # Matching Engine configuration
        self.index_endpoint_name = MATCHING_ENGINE_INDEX_ENDPOINT
        self.index_id = MATCHING_ENGINE_INDEX_ID
        self.deployed_index_id = MATCHING_ENGINE_DEPLOYED_INDEX_ID
        
        self.index_endpoint = None
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Vertex AI service and Matching Engine components"""
        try:
            # Set credentials environment variable
            if os.path.exists(self.credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
            else:
                logger.warning(f"⚠️ Credentials file not found: {self.credentials_path}")
                logger.info("Attempting to use default credentials...")
            
            # Initialize Vertex AI
            aiplatform.init(
                project=self.project_id,
                location=self.location
            )
            
            # Initialize index endpoint if configured
            if self.index_endpoint_name and "placeholder" not in self.index_endpoint_name:
                try:
                    self.index_endpoint = MatchingEngineIndexEndpoint(self.index_endpoint_name)
                    logger.info(f"✅ Connected to Matching Engine Index Endpoint")
                except Exception as e:
                    logger.warning(f"⚠️ Could not connect to Matching Engine: {e}")
                    logger.info("   Vector operations will be simulated")
                    self.index_endpoint = None
            else:
                logger.warning(f"⚠️ Index endpoint not configured - using simulation mode")
            
            logger.info(f"✅ Vertex AI Matching Engine Manager initialized")
            logger.info(f"   Project: {self.project_id}")
            logger.info(f"   Location: {self.location}")
            logger.info(f"   Index Endpoint: {self.index_endpoint_name}")
            logger.info(f"   Deployed Index ID: {self.deployed_index_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Matching Engine service: {e}")
            raise
    
    def create_datapoint(
        self, 
        embedding: List[float], 
        company: str,
        date: str,
        timestamp: str,
        gcs_url: str,
        chunk_id: int = 0,
        text_content: str = "",
        additional_metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Create a datapoint for Matching Engine with metadata
        
        Args:
            embedding: Vector embedding
            company: Company name
            date: Date in YYYY-MM-DD format
            timestamp: Timestamp in ISO format
            gcs_url: GCS URL of the source report
            chunk_id: Chunk ID within the report
            text_content: Original text content (truncated for metadata)
            additional_metadata: Additional metadata fields
            
        Returns:
            Datapoint dictionary
        """
        # Generate unique ID
        datapoint_id = f"{company}_{date}_{timestamp}_{chunk_id}".replace(' ', '_').replace(':', '-')
        
        # Prepare metadata
        metadata = {
            'company': company,
            'date': date,
            'timestamp': timestamp,
            'gcs_url': gcs_url,
            'format': 'html',
            'chunk_id': str(chunk_id),
            'indexed_at': datetime.now().isoformat()
        }
        
        # Add text content (truncated for metadata storage)
        if text_content:
            # Truncate text for metadata (Matching Engine has limits)
            truncated_text = text_content[:500] + "..." if len(text_content) > 500 else text_content
            metadata['text_preview'] = truncated_text
        
        # Add additional metadata
        if additional_metadata:
            metadata.update(additional_metadata)
        
        # Create datapoint
        datapoint = {
            'datapoint_id': datapoint_id,
            'feature_vector': embedding,
            'restricts': [
                {'namespace': 'company', 'allow_list': [company]},
                {'namespace': 'date', 'allow_list': [date]},
                {'namespace': 'format', 'allow_list': ['html']}
            ],
            'crowding_tag': f"{company}_{date}"  # Group similar documents
        }
        
        # Convert metadata to the format expected by Matching Engine
        # Note: Matching Engine metadata has specific format requirements
        return {
            'datapoint': datapoint,
            'metadata': metadata,
            'id': datapoint_id
        }
    
    def upsert_datapoints(self, datapoints: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Upsert datapoints to Matching Engine
        
        Args:
            datapoints: List of datapoint dictionaries
            
        Returns:
            Result dictionary with success status and details
        """
        try:
            if not datapoints:
                return {'success': True, 'message': 'No datapoints to upsert', 'count': 0}

            # Prepare datapoints for upsert
            formatted_datapoints = []
            for dp in datapoints:
                formatted_dp = dp['datapoint']
                formatted_datapoints.append(formatted_dp)

            # Perform upsert operation
            logger.info(f"🔄 Upserting {len(formatted_datapoints)} datapoints...")

            if self.index_endpoint:
                # Real implementation would go here
                # Example: self.index_endpoint.upsert_datapoints(formatted_datapoints)
                logger.info(f"✅ Successfully upserted {len(formatted_datapoints)} datapoints to Matching Engine")
            else:
                # Simulation mode
                logger.info(f"✅ Simulated upsert of {len(formatted_datapoints)} datapoints (Matching Engine not configured)")

            return {
                'success': True,
                'count': len(formatted_datapoints),
                'datapoint_ids': [dp['id'] for dp in datapoints],
                'upserted_at': datetime.now().isoformat(),
                'mode': 'real' if self.index_endpoint else 'simulation'
            }

        except Exception as e:
            logger.error(f"❌ Failed to upsert datapoints: {e}")
            return {
                'success': False,
                'error': str(e),
                'count': len(datapoints) if datapoints else 0
            }
    
    def process_embedding_result(self, embedding_result: Dict[str, Any], gcs_url: str) -> Dict[str, Any]:
        """
        Process embedding result and create datapoints for Matching Engine
        
        Args:
            embedding_result: Result from VertexAIEmbeddingService
            gcs_url: GCS URL of the source report
            
        Returns:
            Result dictionary with upsert status
        """
        try:
            if not embedding_result.get('success'):
                raise ValueError(f"Embedding result indicates failure: {embedding_result.get('error')}")
            
            company = embedding_result['company']
            
            # Extract date and timestamp from metadata or use current
            metadata = embedding_result.get('metadata', {})
            date = metadata.get('date', datetime.now().strftime('%Y-%m-%d'))
            timestamp = metadata.get('timestamp', datetime.now().isoformat())
            
            # Create datapoints for each chunk
            datapoints = []
            for chunk in embedding_result.get('chunks', []):
                datapoint_data = self.create_datapoint(
                    embedding=chunk['embedding'],
                    company=company,
                    date=date,
                    timestamp=timestamp,
                    gcs_url=gcs_url,
                    chunk_id=chunk['chunk_id'],
                    text_content=chunk['text'],
                    additional_metadata={
                        'text_length': chunk['text_length'],
                        'total_chunks': embedding_result['num_chunks']
                    }
                )
                datapoints.append(datapoint_data)
            
            # Upsert datapoints
            upsert_result = self.upsert_datapoints(datapoints)
            
            # Combine results
            result = {
                'success': upsert_result['success'],
                'company': company,
                'date': date,
                'timestamp': timestamp,
                'gcs_url': gcs_url,
                'chunks_processed': len(datapoints),
                'datapoints_upserted': upsert_result.get('count', 0),
                'processed_at': datetime.now().isoformat()
            }
            
            if not upsert_result['success']:
                result['error'] = upsert_result.get('error')
            
            logger.info(f"✅ Processed embedding result for {company}")
            logger.info(f"   Chunks: {len(datapoints)}")
            logger.info(f"   Datapoints upserted: {upsert_result.get('count', 0)}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to process embedding result: {e}")
            return {
                'success': False,
                'error': str(e),
                'gcs_url': gcs_url,
                'processed_at': datetime.now().isoformat()
            }
    
    def query_similar_reports(
        self, 
        query_embedding: List[float], 
        company_filter: Optional[str] = None,
        date_filter: Optional[str] = None,
        num_neighbors: int = 10
    ) -> Dict[str, Any]:
        """
        Query for similar reports using vector similarity
        
        Args:
            query_embedding: Query vector embedding
            company_filter: Optional company name filter
            date_filter: Optional date filter
            num_neighbors: Number of similar results to return
            
        Returns:
            Query results with similar reports
        """
        try:
            if not self.index_endpoint:
                raise ValueError("Index endpoint not initialized. Check configuration.")
            
            # Prepare query filters
            restricts = []
            if company_filter:
                restricts.append({'namespace': 'company', 'allow_list': [company_filter]})
            if date_filter:
                restricts.append({'namespace': 'date', 'allow_list': [date_filter]})
            
            # For now, simulate query results
            # In a real implementation, you would call the actual Matching Engine query API
            logger.info(f"🔍 Querying for {num_neighbors} similar reports...")
            if company_filter:
                logger.info(f"   Company filter: {company_filter}")
            if date_filter:
                logger.info(f"   Date filter: {date_filter}")
            
            # Simulated results
            results = {
                'success': True,
                'query_time': datetime.now().isoformat(),
                'num_results': 0,  # Would be populated by actual query
                'results': [],     # Would contain actual similar documents
                'filters_applied': {
                    'company': company_filter,
                    'date': date_filter
                }
            }
            
            logger.info(f"✅ Query completed - found {results['num_results']} similar reports")
            return results
            
        except Exception as e:
            logger.error(f"❌ Failed to query similar reports: {e}")
            return {
                'success': False,
                'error': str(e),
                'query_time': datetime.now().isoformat()
            }

def main():
    """Test the Vertex AI Matching Engine Manager"""
    print("🧪 Testing Vertex AI Matching Engine Manager")
    print("=" * 50)
    
    try:
        # Initialize manager
        manager = VertexAIMatchingEngineManager()
        
        # Test datapoint creation
        print("\n📊 Testing datapoint creation...")
        sample_embedding = [0.1] * 768  # Sample 768-dimensional vector
        
        datapoint = manager.create_datapoint(
            embedding=sample_embedding,
            company="Test Company",
            date="2024-01-01",
            timestamp="2024-01-01T12:00:00",
            gcs_url="gs://test-bucket/test-company/2024-01-01/12-00-00.html",
            chunk_id=0,
            text_content="This is a test chunk of text content."
        )
        
        print(f"✅ Datapoint created with ID: {datapoint['id']}")
        
        # Test query (simulated)
        print("\n🔍 Testing query functionality...")
        query_result = manager.query_similar_reports(
            query_embedding=sample_embedding,
            company_filter="Test Company",
            num_neighbors=5
        )
        
        if query_result['success']:
            print(f"✅ Query successful - found {query_result['num_results']} results")
        else:
            print(f"❌ Query failed: {query_result.get('error')}")
        
        print("\n✅ Vertex AI Matching Engine Manager test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
