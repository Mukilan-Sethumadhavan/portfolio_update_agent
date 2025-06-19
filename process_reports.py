#!/usr/bin/env python3
"""
Process Reports Script
Uploads generated reports to GCS and processes them through the RAG pipeline
"""

from market_intelligence_pipeline import MarketIntelligencePipeline
import os

def main():
    print("🚀 Processing IronPDF Reports to GCS")
    print("=" * 50)
    
    # Initialize pipeline
    pipeline = MarketIntelligencePipeline()
    
    # List of reports to process
    reports = [
        ('portfolio_report_ironpdf.html', 'ironpdf'),
        ('strategic_analysis_ironpdf_20250618_201701.html', 'ironpdf')
    ]
    
    for html_file, company_name in reports:
        if os.path.exists(html_file):
            print(f"\n📦 Processing: {html_file}")
            print(f"   Company: {company_name}")
            
            try:
                result = pipeline.process_report(html_file, company_name)
                
                if result['success']:
                    print(f"✅ Success! GCS URL: {result.get('final_gcs_url', 'N/A')}")
                    print(f"   Chunks processed: {result.get('chunks_processed', 0)}")
                    print(f"   Vectors stored: {result.get('vectors_stored', 0)}")
                    print(f"   Duration: {result.get('duration_seconds', 0):.2f} seconds")
                else:
                    print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                print(f"❌ Exception: {e}")
        else:
            print(f"⚠️ File not found: {html_file}")
    
    print(f"\n📊 Final Statistics:")
    stats = pipeline.get_pipeline_statistics()
    print(f"   Total reports processed: {stats['statistics'].get('total_reports_processed', 0)}")
    print(f"   Successful runs: {stats['statistics'].get('successful_runs', 0)}")
    print(f"   Failed runs: {stats['statistics'].get('failed_runs', 0)}")

if __name__ == "__main__":
    main() 