#!/usr/bin/env python3
"""
Master Controller - Orchestrates all data collection and report generation

This script:
1. Gets company name from user ONCE
2. Runs main workflow (blog, google, reddit, youtube)
3. Runs LinkedIn scraper independently 
4. Runs Gmail scraper independently
5. Merges all data and generates final comprehensive HTML report
"""

import os
import sys
import json
import subprocess
import time
from datetime import datetime
from typing import List, Dict, Any

# Import the new Market Intelligence Pipeline
try:
    from market_intelligence_pipeline import MarketIntelligencePipeline
    PIPELINE_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Market Intelligence Pipeline not available: {e}")
    print("   Report will be generated but not processed through GCP pipeline")
    PIPELINE_AVAILABLE = False

class MasterController:
    def __init__(self, enable_gcp_pipeline: bool = True):
        self.company_name = ""
        self.main_data_file = ""
        self.linkedin_data_file = ""
        self.gmail_data_file = ""
        self.final_report_file = ""

        # GCP Pipeline integration
        self.enable_gcp_pipeline = enable_gcp_pipeline and PIPELINE_AVAILABLE
        self.pipeline = None

        if self.enable_gcp_pipeline:
            try:
                self.pipeline = MarketIntelligencePipeline(enable_deduplication=True)
                print("✅ Market Intelligence Pipeline initialized")
            except Exception as e:
                print(f"⚠️ Failed to initialize GCP pipeline: {e}")
                print("   Continuing without GCP pipeline...")
                self.enable_gcp_pipeline = False
        
    def get_company_input(self) -> str:
        """Get company name from user input"""
        print("\n" + "="*60)
        print("🚀 COMPREHENSIVE PORTFOLIO ANALYZER")
        print("="*60)
        print("\nThis will collect data from ALL sources:")
        print("📝 Blog posts")
        print("🔍 Google search results") 
        print("🔴 Reddit posts")
        print("📺 YouTube videos")
        print("🔗 LinkedIn activities")
        print("📧 Gmail messages")
        print("\n" + "="*60)
        
        while True:
            company_name = input("\nEnter company name: ").strip()
            if company_name:
                return company_name
            print("Please enter a valid company name.")
    
    def run_main_workflow(self, company_name: str) -> bool:
        """Run the main workflow (blog, google, reddit, youtube)"""
        print(f"\n{'='*50}")
        print("🔄 STEP 1: Running Main Workflow")
        print(f"{'='*50}")
        print("This will collect data from:")
        print("📝 Blog posts")
        print("🔍 Google search results")
        print("🔴 Reddit posts")
        print("📺 YouTube videos")

        try:
            # Import the data collection modules
            from data_aggregator import DataAggregator

            # Initialize data containers
            blog_data = []
            google_data = []
            reddit_data = []
            youtube_data = []

            # Collect blog data
            try:
                print(f"\n📝 Collecting blog posts for {company_name}...")
                from blog_post import get_company_blog_data
                blog_data = get_company_blog_data(company_name)
                print(f"✅ Found {len(blog_data)} blog posts")
            except Exception as e:
                print(f"⚠️ Blog collection failed: {e}")

            # Collect Google search data
            try:
                print(f"\n🔍 Collecting Google search results for {company_name}...")
                from google_search import get_company_google_data
                google_data = get_company_google_data(company_name)
                print(f"✅ Found {len(google_data)} Google results")
            except Exception as e:
                print(f"⚠️ Google search failed: {e}")

            # Collect Reddit data
            try:
                print(f"\n🔴 Collecting Reddit posts for {company_name}...")
                from reddit_scrap import get_company_reddit_data
                reddit_data = get_company_reddit_data(company_name)
                print(f"✅ Found {len(reddit_data)} Reddit posts")
            except Exception as e:
                print(f"⚠️ Reddit collection failed: {e}")

            # Collect YouTube data
            try:
                print(f"\n📺 Collecting YouTube videos for {company_name}...")
                from enhanced_youtube_scraping import get_company_youtube_data
                youtube_data = get_company_youtube_data(company_name)
                print(f"✅ Found {len(youtube_data)} YouTube videos")
            except Exception as e:
                print(f"⚠️ YouTube collection failed: {e}")

            # Aggregate all data
            print(f"\n📊 Aggregating data for {company_name}...")
            aggregator = DataAggregator()
            aggregated_data = aggregator.aggregate_company_data(
                company_name=company_name,
                blog_data=blog_data,
                google_data=google_data,
                reddit_data=reddit_data,
                youtube_data=youtube_data
            )

            # Save aggregated data
            filename = f"aggregated_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
            aggregator.save_aggregated_data(aggregated_data, filename)
            self.main_data_file = filename

            print(f"✅ Main workflow completed successfully")
            print(f"   📝 Blog posts: {len(blog_data)}")
            print(f"   🔍 Google results: {len(google_data)}")
            print(f"   🔴 Reddit posts: {len(reddit_data)}")
            print(f"   📺 YouTube videos: {len(youtube_data)}")
            print(f"   📊 Total articles: {aggregated_data['total_articles']}")
            print(f"   💭 Overall sentiment: {aggregated_data['overall_sentiment']}")

            return True

        except Exception as e:
            print(f"❌ Error running main workflow: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_linkedin_scraper(self, company_name: str) -> bool:
        """Run LinkedIn scraper with PhantomBuster"""
        print(f"\n{'='*50}")
        print("🔗 STEP 2: Running LinkedIn Scraper with PhantomBuster")
        print(f"{'='*50}")
        print("This will use PhantomBuster to:")
        print("🔍 Find company LinkedIn URL")
        print("👥 Export employee data")
        print("📊 Analyze recent activities")
        print("⏳ This will take about 2 minutes...")

        try:
            # Import and run LinkedIn scraper directly
            from linkedin_scraper import get_company_linkedin_data

            linkedin_data = get_company_linkedin_data(company_name)

            if linkedin_data and len(linkedin_data) > 0:
                print(f"✅ LinkedIn scraper completed successfully")
                print(f"   📊 Found {len(linkedin_data)} LinkedIn insights")
                self.linkedin_data_file = f"linkedin_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
                return True
            else:
                print(f"⚠️ LinkedIn scraper completed but no data found")
                print("📝 Creating empty LinkedIn data file...")
                self.linkedin_data_file = f"linkedin_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
                with open(self.linkedin_data_file, 'w') as f:
                    json.dump([], f)
                return True

        except Exception as e:
            print(f"⚠️ LinkedIn scraper error: {e}")
            print("📝 Creating empty LinkedIn data file...")
            self.linkedin_data_file = f"linkedin_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
            with open(self.linkedin_data_file, 'w') as f:
                json.dump([], f)
            return True
    
    def run_gmail_scraper(self, company_name: str) -> bool:
        """Run Gmail scraper independently"""
        print(f"\n{'='*50}")
        print("🔄 STEP 3: Running Gmail Scraper")
        print(f"{'='*50}")
        
        try:
            # Run gm.py with company name
            process = subprocess.Popen(
                [sys.executable, "gm.py"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(input=f"{company_name}\n")
            
            if process.returncode == 0:
                print("✅ Gmail scraper completed successfully")
                self.gmail_data_file = f"gmail_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
                return True
            else:
                print(f"⚠️ Gmail scraper had issues: {stderr}")
                print("📝 Creating empty Gmail data file...")
                self.gmail_data_file = f"gmail_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
                with open(self.gmail_data_file, 'w') as f:
                    json.dump([], f)
                return True
                
        except Exception as e:
            print(f"⚠️ Gmail scraper error: {e}")
            print("📝 Creating empty Gmail data file...")
            self.gmail_data_file = f"gmail_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
            with open(self.gmail_data_file, 'w') as f:
                json.dump([], f)
            return True
    
    def merge_and_generate_final_report(self, company_name: str) -> bool:
        """Merge all data sources and generate final comprehensive report"""
        print(f"\n{'='*50}")
        print("🔄 STEP 4: Merging Data & Generating Final Report")
        print(f"{'='*50}")
        
        try:
            # Run merge_reports.py
            process = subprocess.Popen(
                [sys.executable, "merge_reports.py", company_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                print("✅ Final report generated successfully")
                self.final_report_file = f"final_report_{company_name.replace(' ', '_').replace('.', '').lower()}.html"
                return True
            else:
                print(f"❌ Final report generation failed: {stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error generating final report: {e}")
            return False

    def process_through_gcp_pipeline(self, company_name: str) -> bool:
        """
        Process the final report through the GCP Market Intelligence Pipeline

        Args:
            company_name: Company name for the report

        Returns:
            True if successful, False otherwise
        """
        if not self.enable_gcp_pipeline:
            print("⏭️ GCP Pipeline processing skipped (not enabled)")
            return True

        if not self.final_report_file or not os.path.exists(self.final_report_file):
            print(f"❌ Final report file not found: {self.final_report_file}")
            return False

        print(f"\n{'='*50}")
        print("🚀 STEP 5: Processing through GCP Pipeline")
        print(f"{'='*50}")
        print("This will:")
        print("📦 Upload report to Google Cloud Storage")
        print("🧠 Generate embeddings using Vertex AI")
        print("🔍 Store vectors in Matching Engine for RAG")
        print("🔄 Handle deduplication if needed")

        try:
            # Process through pipeline
            pipeline_result = self.pipeline.process_report(self.final_report_file, company_name)

            if pipeline_result['success']:
                print("✅ GCP Pipeline processing completed successfully!")
                print(f"   📊 Final GCS URL: {pipeline_result.get('final_gcs_url')}")
                print(f"   🧠 Chunks processed: {pipeline_result.get('chunks_processed', 0)}")
                print(f"   🔍 Vectors stored: {pipeline_result.get('vectors_stored', 0)}")
                print(f"   ⏱️ Duration: {pipeline_result.get('duration_seconds', 0):.2f} seconds")

                # Show deduplication results if available
                dedup_result = pipeline_result.get('steps', {}).get('deduplication', {})
                if dedup_result and not dedup_result.get('skipped'):
                    duplicates_removed = dedup_result.get('duplicates_removed', 0)
                    if duplicates_removed > 0:
                        print(f"   🔄 Duplicates removed: {duplicates_removed}")
                    else:
                        print(f"   🔄 No duplicates found")

                return True
            else:
                print(f"❌ GCP Pipeline processing failed: {pipeline_result.get('error')}")
                print("📋 Pipeline steps completed:")
                for step_name, step_result in pipeline_result.get('steps', {}).items():
                    status = "✅" if step_result.get('success', False) else "❌"
                    print(f"   {status} {step_name}")
                return False

        except Exception as e:
            print(f"❌ Error during GCP pipeline processing: {e}")
            return False

    def run(self):
        """Main execution flow"""
        try:
            # Get company name from user
            self.company_name = self.get_company_input()
            
            print(f"\n🎯 Processing comprehensive analysis for: {self.company_name}")
            print("⏱️ This will take several minutes due to LinkedIn processing...")
            
            # Step 1: Run main workflow
            if not self.run_main_workflow(self.company_name):
                print("❌ Main workflow failed. Exiting.")
                return
            
            # Step 2: Run LinkedIn scraper
            if not self.run_linkedin_scraper(self.company_name):
                print("❌ LinkedIn scraper failed. Exiting.")
                return
            
            # Step 3: Run Gmail scraper  
            if not self.run_gmail_scraper(self.company_name):
                print("❌ Gmail scraper failed. Exiting.")
                return
            
            # Step 4: Merge and generate final report
            if not self.merge_and_generate_final_report(self.company_name):
                print("❌ Final report generation failed. Exiting.")
                return

            # Step 5: Process through GCP Pipeline (if enabled)
            if not self.process_through_gcp_pipeline(self.company_name):
                print("⚠️ GCP Pipeline processing failed, but continuing...")
                print("📊 Local report is still available")

            # Success summary
            print(f"\n{'='*60}")
            print("🎉 COMPREHENSIVE ANALYSIS COMPLETE!")
            print(f"{'='*60}")
            print(f"Company: {self.company_name}")
            print(f"Main data: {self.main_data_file}")
            print(f"LinkedIn data: {self.linkedin_data_file}")
            print(f"Gmail data: {self.gmail_data_file}")
            print(f"📊 Final Report: {self.final_report_file}")

            # Show GCP pipeline status
            if self.enable_gcp_pipeline:
                print(f"🚀 GCP Pipeline: Enabled and processed")
                print(f"   📦 Report stored in Google Cloud Storage")
                print(f"   🧠 Embeddings generated with Vertex AI")
                print(f"   🔍 Vectors stored in Matching Engine for RAG")
            else:
                print(f"⏭️ GCP Pipeline: Disabled or unavailable")

            print(f"{'='*60}")
            
            # Open final report
            try:
                import webbrowser
                open_browser = input(f"\nOpen final report in browser? (y/n): ").strip().lower()
                if open_browser in ['y', 'yes']:
                    webbrowser.open(f"file://{os.path.abspath(self.final_report_file)}")
            except ImportError:
                pass
                
        except KeyboardInterrupt:
            print("\n\n⚠️ Operation cancelled by user.")
        except Exception as e:
            print(f"\n❌ An error occurred: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Entry point"""
    # Check if user wants to enable GCP pipeline
    enable_gcp = True
    if len(sys.argv) > 1 and sys.argv[1] == '--no-gcp':
        enable_gcp = False
        print("🔧 GCP Pipeline disabled via command line argument")

    controller = MasterController(enable_gcp_pipeline=enable_gcp)
    controller.run()

if __name__ == "__main__":
    main()
