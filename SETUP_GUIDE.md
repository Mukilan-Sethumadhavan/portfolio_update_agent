# Market Intelligence Pipeline - Complete Setup Guide

This guide will help you set up the complete Market Intelligence Pipeline with Google Cloud Platform integration.

## 🏗️ Architecture Overview

The pipeline consists of:
1. **Report Generation**: `master_controller.py` generates HTML market reports
2. **GCS Storage**: Reports stored in structured paths `gs://bucket/{company}/{YYYY-MM-DD}/{HH-MM-SS}.html`
3. **Vertex AI Embeddings**: HTML content converted to vectors using `textembedding-gecko@003`
4. **Matching Engine**: Vectors stored for semantic search (RAG)
5. **Deduplication**: Latest-timestamp-wins logic for multiple reports per day

## 📋 Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account
- GCP Project with billing enabled
- Basic knowledge of GCP services

## 🚀 Quick Start

### Step 1: Clone and Install Dependencies

```bash
# Navigate to your project directory
cd "c:\Users\101928\Downloads\news summarizer9"

# Install Python dependencies
pip install -r requirements.txt
```

### Step 2: GCP Setup

Follow the detailed [GCP Credentials Setup Guide](gcp_credentials_setup.md):

1. **Create GCP Project** (if needed)
2. **Enable APIs**:
   ```bash
   gcloud services enable storage.googleapis.com
   gcloud services enable aiplatform.googleapis.com
   gcloud services enable compute.googleapis.com
   ```

3. **Create Service Accounts**:
   - Storage Service Account → `gcp_storage_credentials.json`
   - Vertex AI Service Account → `gcp_vertex_ai_credentials.json`

4. **Create GCS Bucket**:
   - Choose globally unique name
   - Recommended region: `us-central1`

5. **Update Configuration**:
   - Edit `gcp_project_config.py`
   - Set your project ID and bucket name

### Step 3: Validate Setup

```bash
# Validate GCP credentials and configuration
python gcp_credentials_validator.py
```

### Step 4: Test Individual Components

```bash
# Test GCS Storage Manager
python gcs_storage_manager.py

# Test Vertex AI Embedding Service
python vertex_ai_embedding_service.py

# Test Vertex AI Matching Engine Manager
python vertex_ai_matching_engine.py

# Test Complete Pipeline
python market_intelligence_pipeline.py
```

### Step 5: Run Complete Analysis

```bash
# Run with GCP pipeline enabled (default)
python master_controller.py

# Run without GCP pipeline (local only)
python master_controller.py --no-gcp
```

## 📁 File Structure

```
news summarizer9/
├── master_controller.py              # Main orchestrator (UPDATED)
├── market_intelligence_pipeline.py   # GCP pipeline orchestrator
├── gcs_storage_manager.py            # Google Cloud Storage operations
├── vertex_ai_embedding_service.py    # Vertex AI embedding generation
├── vertex_ai_matching_engine.py      # Matching Engine vector storage
├── report_deduplication_manager.py   # Deduplication logic
├── config.py                         # Configuration (UPDATED)
├── gcp_project_config.py             # GCP-specific configuration
├── gcp_credentials_setup.md          # Detailed GCP setup guide
├── gcp_credentials_validator.py      # Validation script
├── requirements.txt                  # Dependencies (UPDATED)
├── gcp_storage_credentials.json      # GCS service account (YOU CREATE)
├── gcp_vertex_ai_credentials.json    # Vertex AI service account (YOU CREATE)
└── [existing files...]               # Your existing scraping components
```

## 🔧 Configuration Files

### Required Files You Need to Create:

1. **`gcp_storage_credentials.json`** - GCS service account credentials
2. **`gcp_vertex_ai_credentials.json`** - Vertex AI service account credentials
3. **Update `gcp_project_config.py`** - Set your project ID and bucket name

### Configuration Hierarchy:

1. `config.py` - General configuration and GCP defaults
2. `gcp_project_config.py` - Project-specific GCP settings
3. Credential JSON files - Service account authentication

## 🎯 Usage Examples

### Basic Usage (Recommended)

```bash
python master_controller.py
```

This will:
1. Ask for company name (e.g., "Tesla")
2. Run all scrapers (blog, Google, Reddit, YouTube, LinkedIn, Gmail)
3. Generate comprehensive HTML report
4. **NEW**: Upload to GCS, generate embeddings, store vectors
5. **NEW**: Handle deduplication automatically

### Advanced Usage

```python
from market_intelligence_pipeline import MarketIntelligencePipeline

# Initialize pipeline
pipeline = MarketIntelligencePipeline(enable_deduplication=True)

# Process existing HTML report
result = pipeline.process_report("final_report_tesla.html", "Tesla")

if result['success']:
    print(f"GCS URL: {result['final_gcs_url']}")
    print(f"Vectors stored: {result['vectors_stored']}")
```

### Deduplication Only

```python
from report_deduplication_manager import ReportDeduplicationManager

manager = ReportDeduplicationManager()
result = manager.deduplicate_company_reports("Tesla", "2024-01-15")
```

## 🔍 Monitoring and Logs

The pipeline creates several log files:

- `pipeline_log.json` - Pipeline execution history
- `deduplication_log.json` - Deduplication operations
- Console output with detailed progress

### Check Pipeline Statistics

```python
from market_intelligence_pipeline import MarketIntelligencePipeline

pipeline = MarketIntelligencePipeline()
stats = pipeline.get_pipeline_statistics()
print(f"Total reports processed: {stats['statistics']['total_reports_processed']}")
```

## 🚨 Troubleshooting

### Common Issues:

1. **"GCP credentials not found"**
   - Ensure credential JSON files exist
   - Check file paths in configuration
   - Run `python gcp_credentials_validator.py`

2. **"API not enabled"**
   - Enable required APIs in GCP Console
   - Wait a few minutes for propagation

3. **"Permission denied"**
   - Check service account roles
   - Ensure bucket exists and is accessible

4. **"Pipeline components not initialized"**
   - Run health check: `pipeline.health_check()`
   - Check individual component tests

### Debug Mode:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Then run your pipeline
```

## 💰 Cost Considerations

### GCP Services Used:
- **Cloud Storage**: ~$0.02/GB/month
- **Vertex AI Embeddings**: ~$0.0001 per 1K characters
- **Matching Engine**: ~$0.50/hour when deployed

### Cost Optimization:
- Use regional storage (cheaper than multi-regional)
- Batch embedding requests when possible
- Consider Matching Engine deployment schedule

## 🔒 Security Best Practices

1. **Never commit credential files to version control**
2. **Use least-privilege service accounts**
3. **Regularly rotate service account keys**
4. **Monitor GCP usage and billing**
5. **Use VPC and firewall rules in production**

## 📞 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Run validation scripts
3. Check GCP Console for service status
4. Review log files for detailed error messages

## 🎉 What's Next?

After setup, you can:
1. Run market intelligence analysis for any company
2. Query similar reports using vector search
3. Build RAG applications on top of stored vectors
4. Extend the pipeline with additional data sources
