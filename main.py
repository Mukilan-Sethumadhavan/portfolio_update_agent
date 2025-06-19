#!/usr/bin/env python3
"""
Portfolio Performance Analyzer - Main Entry Point

This script orchestrates data collection from multiple sources (blog posts, Google search,
Reddit, YouTube) and generates executive-level portfolio reports using Gemini AI.

Usage:
    python main.py

The script will prompt for company names and generate HTML reports.
"""

import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any

# Import our custom modules
from data_aggregator import DataAggregator
from gemini_processor import GeminiProcessor
from portfolio_synthesizer import PortfolioSynthesizer

# Import data collection modules
try:
    from blog_post import get_company_blog_data
except ImportError as e:
    print(f"Warning: Blog post module not available: {e}")
    get_company_blog_data = None

try:
    from google_search import get_company_google_data
except ImportError as e:
    print(f"Warning: Google search module not available: {e}")
    get_company_google_data = None

try:
    from reddit_scrap import get_company_reddit_data
except ImportError as e:
    print(f"Warning: Reddit module not available: {e}")
    get_company_reddit_data = None

try:
    from enhanced_youtube_scraping import get_company_youtube_data
except ImportError as e:
    print(f"Warning: YouTube module not available: {e}")
    get_company_youtube_data = None

try:
    from linkedin_scraper import get_company_linkedin_data
except ImportError as e:
    print(f"Warning: LinkedIn module not available: {e}")
    get_company_linkedin_data = None


class PortfolioAnalyzer:
    def __init__(self):
        self.aggregator = DataAggregator()
        self.gemini_processor = GeminiProcessor()
        self.synthesizer = PortfolioSynthesizer()
        self.results = {}

    def get_company_input(self) -> List[str]:
        """Get company names from user input"""
        print("\n" + "="*60)
        print("Portfolio Performance Analyzer")
        print("="*60)
        print("\nEnter company names to analyze.")
        print("You can enter multiple companies separated by commas.")
        print("Example: Apple, Microsoft, Tesla")
        print("\nPress Enter with empty input to finish.")

        companies = []
        while True:
            company_input = input("\nEnter company name(s): ").strip()

            if not company_input:
                if companies:
                    break
                else:
                    print("Please enter at least one company name.")
                    continue

            # Split by comma and clean up
            new_companies = [c.strip() for c in company_input.split(',') if c.strip()]
            companies.extend(new_companies)

            print(f"Added: {', '.join(new_companies)}")
            print(f"Total companies: {len(companies)}")

            continue_input = input("Add more companies? (y/n): ").strip().lower()
            if continue_input not in ['y', 'yes']:
                break

        # Remove duplicates while preserving order
        unique_companies = []
        for company in companies:
            if company not in unique_companies:
                unique_companies.append(company)

        return unique_companies

    def collect_company_data(self, company_name: str) -> Dict[str, Any]:
        """Collect data from all sources for a single company"""
        print(f"\n{'='*50}")
        print(f"Collecting data for: {company_name}")
        print(f"{'='*50}")

        # Initialize data containers
        blog_data = []
        google_data = []
        reddit_data = []
        youtube_data = []
        linkedin_data = []

        # Collect blog data
        if get_company_blog_data:
            try:
                print("\n📝 Collecting blog posts...")
                blog_data = get_company_blog_data(company_name)
            except Exception as e:
                print(f"Error collecting blog data: {e}")

        # Collect Google search data
        if get_company_google_data:
            try:
                print("\n🔍 Collecting Google search results...")
                google_data = get_company_google_data(company_name)
            except Exception as e:
                print(f"Error collecting Google data: {e}")

        # Collect Reddit data
        if get_company_reddit_data:
            try:
                print("\n🔴 Collecting Reddit posts...")
                reddit_data = get_company_reddit_data(company_name)
            except Exception as e:
                print(f"Error collecting Reddit data: {e}")

        # Collect YouTube data
        if get_company_youtube_data:
            try:
                print("\n📺 Collecting YouTube videos...")
                youtube_data = get_company_youtube_data(company_name)
            except Exception as e:
                print(f"Error collecting YouTube data: {e}")

        # Collect LinkedIn data
        if get_company_linkedin_data:
            try:
                print("\n👥 Collecting LinkedIn posts...")
                linkedin_data = get_company_linkedin_data(company_name)
            except Exception as e:
                print(f"Error collecting LinkedIn data: {e}")

        # Aggregate all data
        print("\n[DATA] Aggregating data...")
        aggregated_data = self.aggregator.aggregate_company_data(
            company_name=company_name,
            blog_data=blog_data,
            google_data=google_data,
            reddit_data=reddit_data,
            youtube_data=youtube_data,
            linkedin_data=linkedin_data
        )

        # Save aggregated data
        filename = f"aggregated_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        self.aggregator.save_aggregated_data(aggregated_data, filename)

        print(f"\n[SUCCESS] Data collection complete for {company_name}")
        print(f"   Total articles: {aggregated_data['total_articles']}")
        print(f"   Overall sentiment: {aggregated_data['overall_sentiment']}")

        return aggregated_data

    def generate_reports(self, companies_data: Dict[str, Dict[str, Any]]) -> str:
        """Generate HTML reports for all companies"""
        print(f"\n{'='*50}")
        print("Generating Portfolio Reports")
        print(f"{'='*50}")

        company_reports = {}

        # Generate individual company reports
        for company_name, company_data in companies_data.items():
            print(f"\n[AI] Generating report for {company_name}...")
            try:
                html_report = self.gemini_processor.generate_company_report(company_data)
                company_reports[company_name] = html_report
                print(f"[SUCCESS] Report generated for {company_name}")
            except Exception as e:
                print(f"[ERROR] Error generating report for {company_name}: {e}")
                # Create fallback report
                company_reports[company_name] = self.gemini_processor.create_fallback_html(company_data)

        # Combine reports if multiple companies
        if len(company_reports) == 1:
            # Single company - use individual report
            company_name = list(company_reports.keys())[0]
            final_html = company_reports[company_name]
            output_filename = f"portfolio_report_{company_name.replace(' ', '_').replace('.', '').lower()}.html"
        else:
            # Multiple companies - combine reports
            print(f"\n[COMBINE] Combining reports for {len(company_reports)} companies...")
            final_html = self.gemini_processor.combine_company_reports(company_reports)
            output_filename = f"portfolio_report_multi_company_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        # Save final report
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(final_html)

        print(f"\n[SUCCESS] Portfolio report saved to: {output_filename}")

        # Always generate strategic analysis (single company) or portfolio synthesis (multiple companies)
        analysis_type = "strategic analysis" if len(companies_data) == 1 else "portfolio synthesis"
        print(f"\n[ANALYSIS] Generating {analysis_type}...")
        try:
            synthesis_filename = self.synthesizer.save_portfolio_synthesis(companies_data)
            return output_filename, synthesis_filename
        except Exception as e:
            print(f"[ERROR] Error generating {analysis_type}: {e}")
            return output_filename, None

    def run(self):
        """Main execution flow"""
        try:
            # Get company names from user
            companies = self.get_company_input()

            if not companies:
                print("No companies provided. Exiting.")
                return

            print(f"\nAnalyzing {len(companies)} companies: {', '.join(companies)}")

            # Collect data for all companies
            companies_data = {}
            for company in companies:
                companies_data[company] = self.collect_company_data(company)

            # Generate reports
            result = self.generate_reports(companies_data)
            if isinstance(result, tuple):
                output_file, synthesis_file = result
            else:
                output_file, synthesis_file = result, None

            # Summary
            print(f"\n{'='*60}")
            print("ANALYSIS COMPLETE")
            print(f"{'='*60}")
            print(f"Companies analyzed: {len(companies)}")
            print(f"Individual report: {output_file}")
            if synthesis_file:
                analysis_type = "Strategic analysis" if len(companies) == 1 else "Portfolio synthesis"
                print(f"{analysis_type}: {synthesis_file}")
            print(f"Total articles processed: {sum(data['total_articles'] for data in companies_data.values())}")

            # Open reports in browser (optional)
            try:
                import webbrowser
                open_browser = input(f"\nOpen reports in browser? (y/n): ").strip().lower()
                if open_browser in ['y', 'yes']:
                    webbrowser.open(f"file://{os.path.abspath(output_file)}")
                    if synthesis_file:
                        webbrowser.open(f"file://{os.path.abspath(synthesis_file)}")
            except ImportError:
                pass

        except KeyboardInterrupt:
            print("\n\nOperation cancelled by user.")
        except Exception as e:
            print(f"\n[ERROR] An error occurred: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Entry point"""
    analyzer = PortfolioAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()
