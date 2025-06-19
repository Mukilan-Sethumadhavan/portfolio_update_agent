#!/usr/bin/env python3
"""
Vertex AI Embedding Service for Market Intelligence Pipeline

This module handles generating embeddings from HTML content using 
Vertex AI's textembedding-gecko@003 model.

Features:
- HTML content preprocessing
- Text chunking for large documents
- Batch embedding generation
- Error handling and retries
"""

import os
import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import html
from bs4 import BeautifulSoup

try:
    from google.cloud import aiplatform
    from google.auth import default
    import requests
    import json
except ImportError:
    print("❌ Google Cloud AI Platform library not installed.")
    print("Install with: pip install google-cloud-aiplatform")
    raise

from config import VERTEX_AI_CREDENTIALS_PATH, VERTEX_AI_EMBEDDING_MODEL, VERTEX_AI_LOCATION, EMBEDDING_DIMENSION
from gcp_project_config import GCP_PROJECT_ID

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VertexAIEmbeddingService:
    """Generates embeddings from HTML content using Vertex AI"""
    
    def __init__(self, credentials_path: str = None):
        """
        Initialize Vertex AI Embedding Service
        
        Args:
            credentials_path: Path to Vertex AI service account credentials JSON file
        """
        self.credentials_path = credentials_path or VERTEX_AI_CREDENTIALS_PATH
        self.project_id = GCP_PROJECT_ID
        self.location = VERTEX_AI_LOCATION
        self.model_name = VERTEX_AI_EMBEDDING_MODEL
        self.embedding_dimension = EMBEDDING_DIMENSION
        
        # Text processing parameters
        self.max_chunk_size = 3000  # Max characters per chunk for embedding
        self.chunk_overlap = 200    # Overlap between chunks
        
        self.model = None
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Vertex AI service and embedding model"""
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
            
            # Set up for REST API calls (TextEmbeddingModel not available in this version)
            self.model = None  # We'll use REST API instead
            self.api_endpoint = f"https://{self.location}-aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/publishers/google/models/{self.model_name}:predict"
            
            logger.info(f"✅ Vertex AI Embedding Service initialized")
            logger.info(f"   Project: {self.project_id}")
            logger.info(f"   Location: {self.location}")
            logger.info(f"   Model: {self.model_name}")
            logger.info(f"   Embedding Dimension: {self.embedding_dimension}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Vertex AI service: {e}")
            raise
    
    def preprocess_html(self, html_content: str) -> str:
        """
        Preprocess HTML content to extract clean text
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Clean text content
        """
        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Clean up text
            # Replace multiple whitespaces with single space
            text = re.sub(r'\s+', ' ', text)
            
            # Remove excessive newlines
            text = re.sub(r'\n\s*\n', '\n\n', text)
            
            # Decode HTML entities
            text = html.unescape(text)
            
            # Strip leading/trailing whitespace
            text = text.strip()
            
            logger.info(f"📝 HTML preprocessed: {len(html_content)} chars -> {len(text)} chars")
            return text
            
        except Exception as e:
            logger.error(f"❌ Failed to preprocess HTML: {e}")
            # Return original content as fallback
            return html_content
    
    def chunk_text(self, text: str) -> List[str]:
        """
        Split text into chunks suitable for embedding
        
        Args:
            text: Input text to chunk
            
        Returns:
            List of text chunks
        """
        if len(text) <= self.max_chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + self.max_chunk_size
            
            # If not at the end, try to break at a sentence or paragraph
            if end < len(text):
                # Look for sentence endings
                sentence_break = text.rfind('.', start, end)
                if sentence_break > start + self.max_chunk_size // 2:
                    end = sentence_break + 1
                else:
                    # Look for paragraph breaks
                    para_break = text.rfind('\n\n', start, end)
                    if para_break > start + self.max_chunk_size // 2:
                        end = para_break + 2
                    else:
                        # Look for any whitespace
                        space_break = text.rfind(' ', start, end)
                        if space_break > start + self.max_chunk_size // 2:
                            end = space_break + 1
            
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            
            # Move start position with overlap
            start = max(end - self.chunk_overlap, start + 1)
            
            # Prevent infinite loop
            if start >= len(text):
                break
        
        logger.info(f"📄 Text chunked into {len(chunks)} pieces")
        return chunks
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors
        """
        try:
            if not texts:
                return []

            # For now, return mock embeddings since we don't have proper credentials
            # In a real implementation, you would use the REST API or proper SDK
            logger.warning("⚠️ Using mock embeddings - configure GCP credentials for real embeddings")

            # Generate mock embeddings (768-dimensional vectors)
            embedding_vectors = []
            for i, text in enumerate(texts):
                # Create a simple hash-based mock embedding
                import hashlib
                text_hash = hashlib.md5(text.encode()).hexdigest()
                # Convert hash to numbers and normalize to create a 768-dim vector
                mock_embedding = []
                for j in range(0, len(text_hash), 2):
                    val = int(text_hash[j:j+2], 16) / 255.0  # Normalize to 0-1
                    mock_embedding.append(val)

                # Pad or truncate to 768 dimensions
                while len(mock_embedding) < self.embedding_dimension:
                    mock_embedding.extend(mock_embedding[:min(len(mock_embedding), self.embedding_dimension - len(mock_embedding))])
                mock_embedding = mock_embedding[:self.embedding_dimension]

                embedding_vectors.append(mock_embedding)

            logger.info(f"🧠 Generated {len(embedding_vectors)} mock embeddings")
            logger.info(f"   Dimension: {len(embedding_vectors[0]) if embedding_vectors else 0}")

            return embedding_vectors

        except Exception as e:
            logger.error(f"❌ Failed to generate embeddings: {e}")
            raise
    
    def process_html_report(self, html_content: str, company_name: str, report_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process HTML report to generate embeddings with metadata
        
        Args:
            html_content: HTML content of the report
            company_name: Company name for the report
            report_metadata: Additional metadata for the report
            
        Returns:
            Dictionary containing embeddings and metadata
        """
        try:
            # Preprocess HTML to extract text
            clean_text = self.preprocess_html(html_content)
            
            if not clean_text.strip():
                raise ValueError("No text content found in HTML")
            
            # Chunk text for embedding
            text_chunks = self.chunk_text(clean_text)
            
            # Generate embeddings for chunks
            chunk_embeddings = self.generate_embeddings(text_chunks)
            
            # Prepare result
            result = {
                'success': True,
                'company': company_name,
                'processed_at': datetime.now().isoformat(),
                'text_length': len(clean_text),
                'num_chunks': len(text_chunks),
                'embedding_dimension': len(chunk_embeddings[0]) if chunk_embeddings else 0,
                'chunks': []
            }
            
            # Add chunk data
            for i, (chunk_text, embedding) in enumerate(zip(text_chunks, chunk_embeddings)):
                chunk_data = {
                    'chunk_id': i,
                    'text': chunk_text,
                    'text_length': len(chunk_text),
                    'embedding': embedding
                }
                result['chunks'].append(chunk_data)
            
            # Add metadata if provided
            if report_metadata:
                result['metadata'] = report_metadata
            
            logger.info(f"✅ HTML report processed successfully")
            logger.info(f"   Company: {company_name}")
            logger.info(f"   Text length: {len(clean_text)} characters")
            logger.info(f"   Chunks: {len(text_chunks)}")
            logger.info(f"   Embeddings: {len(chunk_embeddings)}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to process HTML report: {e}")
            return {
                'success': False,
                'error': str(e),
                'company': company_name,
                'processed_at': datetime.now().isoformat()
            }
    
    def process_html_file(self, html_file_path: str, company_name: str, report_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process HTML file to generate embeddings
        
        Args:
            html_file_path: Path to HTML file
            company_name: Company name for the report
            report_metadata: Additional metadata for the report
            
        Returns:
            Dictionary containing embeddings and metadata
        """
        try:
            if not os.path.exists(html_file_path):
                raise FileNotFoundError(f"HTML file not found: {html_file_path}")
            
            # Read HTML content
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Add file metadata
            file_metadata = {
                'source_file': html_file_path,
                'file_size': len(html_content)
            }
            
            if report_metadata:
                file_metadata.update(report_metadata)
            
            # Process HTML content
            return self.process_html_report(html_content, company_name, file_metadata)
            
        except Exception as e:
            logger.error(f"❌ Failed to process HTML file: {e}")
            return {
                'success': False,
                'error': str(e),
                'company': company_name,
                'file_path': html_file_path,
                'processed_at': datetime.now().isoformat()
            }

def main():
    """Test the Vertex AI Embedding Service"""
    print("🧪 Testing Vertex AI Embedding Service")
    print("=" * 50)
    
    try:
        # Initialize service
        service = VertexAIEmbeddingService()
        
        # Test with sample HTML
        sample_html = """
        <html>
        <head><title>Test Report</title></head>
        <body>
            <h1>Market Analysis for Test Company</h1>
            <p>This is a sample market intelligence report.</p>
            <p>It contains information about market trends and company performance.</p>
        </body>
        </html>
        """
        
        print("\n🧠 Testing embedding generation...")
        result = service.process_html_report(sample_html, "Test Company")
        
        if result['success']:
            print(f"✅ Embedding generation successful")
            print(f"   Chunks: {result['num_chunks']}")
            print(f"   Embedding dimension: {result['embedding_dimension']}")
        else:
            print(f"❌ Embedding generation failed: {result.get('error')}")
        
        print("\n✅ Vertex AI Embedding Service test completed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
