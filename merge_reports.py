#!/usr/bin/env python3
"""
Merge Reports - Combines all data sources and generates final comprehensive HTML report

This script:
1. Loads main workflow data (blog, google, reddit, youtube)
2. Loads LinkedIn data (if available)
3. Loads Gmail data (if available)
4. Merges all data sources
5. Generates comprehensive HTML report with all sources
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any, List
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class ReportMerger:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
        
    def analyze_sentiment(self, text: str):
        """Analyze sentiment of text"""
        if not text:
            return 'neutral', 0.0
        
        scores = self.analyzer.polarity_scores(text)
        compound = scores['compound']
        
        if compound >= 0.05:
            return 'positive', compound
        elif compound <= -0.05:
            return 'negative', compound
        else:
            return 'neutral', compound
    
    def standardize_linkedin_data(self, linkedin_data: List[Dict]) -> List[Dict]:
        """Convert LinkedIn data to standard format"""
        standardized = []
        for item in linkedin_data:
            content = f"{item.get('headline', '')} {item.get('description', '')} {item.get('full_content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': item.get('headline', ''),
                'description': item.get('description', ''),
                'url': item.get('url', ''),
                'image_url': item.get('image_url', ''),
                'full_content': item.get('full_content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"LinkedIn activity: {item.get('headline', '')}",
                'source': 'linkedin',
                'date': item.get('date', 'Unknown')
            })
        return standardized
    
    def standardize_gmail_data(self, gmail_data: List[Dict]) -> List[Dict]:
        """Convert Gmail data to standard format"""
        standardized = []
        for email in gmail_data:
            content = f"{email.get('headline', '')} {email.get('description', '')} {email.get('full_content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': email.get('headline', ''),
                'description': email.get('description', ''),
                'url': email.get('url', ''),
                'image_url': email.get('image_url', ''),
                'full_content': email.get('full_content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"Email: {email.get('headline', '')}",
                'source': 'gmail',
                'date': email.get('date', 'Unknown'),
                'sender': email.get('sender', 'Unknown')
            })
        return standardized
    
    def load_data_file(self, filename: str) -> Any:
        """Load JSON data file if it exists"""
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"[SUCCESS] Loaded {filename}")
                return data
            except Exception as e:
                print(f"[WARNING] Error loading {filename}: {e}")
                return None
        else:
            print(f"[WARNING] File not found: {filename}")
            return None
    
    def merge_all_data(self, company_name: str) -> Dict[str, Any]:
        """Merge all data sources into comprehensive dataset"""
        print(f"\n[MERGE] Merging all data sources for {company_name}...")
        
        # Load main workflow data
        main_file = f"aggregated_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        main_data = self.load_data_file(main_file)
        
        if not main_data:
            raise Exception(f"Main data file {main_file} not found or invalid")
        
        # Load LinkedIn data
        linkedin_file = f"linkedin_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        linkedin_raw = self.load_data_file(linkedin_file) or []
        
        # Load Gmail data
        gmail_file = f"gmail_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        gmail_raw = self.load_data_file(gmail_file) or []
        
        # Standardize additional data
        linkedin_data = self.standardize_linkedin_data(linkedin_raw)
        gmail_data = self.standardize_gmail_data(gmail_raw)
        
        # Merge all articles
        all_articles = main_data.get('articles', [])
        all_articles.extend(linkedin_data)
        all_articles.extend(gmail_data)
        
        # Recalculate overall sentiment
        if all_articles:
            avg_sentiment_score = sum(article['sentiment_score'] for article in all_articles) / len(all_articles)
            overall_sentiment = 'positive' if avg_sentiment_score >= 0.05 else 'negative' if avg_sentiment_score <= -0.05 else 'neutral'
        else:
            overall_sentiment = 'neutral'
            avg_sentiment_score = 0.0
        
        # Create comprehensive dataset
        merged_data = {
            'company_name': company_name,
            'overall_sentiment': overall_sentiment,
            'overall_sentiment_score': avg_sentiment_score,
            'total_articles': len(all_articles),
            'articles': all_articles,
            'sources_count': {
                'blog': len([a for a in all_articles if a['source'] == 'blog']),
                'google_search': len([a for a in all_articles if a['source'] == 'google_search']),
                'reddit': len([a for a in all_articles if a['source'] == 'reddit']),
                'youtube': len([a for a in all_articles if a['source'] == 'youtube']),
                'linkedin': len([a for a in all_articles if a['source'] == 'linkedin']),
                'gmail': len([a for a in all_articles if a['source'] == 'gmail'])
            },
            'generation_time': datetime.now().isoformat(),
            'data_sources': {
                'main_workflow': main_file,
                'linkedin_data': linkedin_file if linkedin_raw else None,
                'gmail_data': gmail_file if gmail_raw else None
            }
        }
        
        print(f"[SUMMARY] Merged data summary:")
        print(f"   Total articles: {merged_data['total_articles']}")
        print(f"   Blog: {merged_data['sources_count']['blog']}")
        print(f"   Google: {merged_data['sources_count']['google_search']}")
        print(f"   Reddit: {merged_data['sources_count']['reddit']}")
        print(f"   YouTube: {merged_data['sources_count']['youtube']}")
        print(f"   LinkedIn: {merged_data['sources_count']['linkedin']}")
        print(f"   Gmail: {merged_data['sources_count']['gmail']}")
        print(f"   Overall sentiment: {merged_data['overall_sentiment']}")
        
        return merged_data
    
    def generate_comprehensive_html_report(self, merged_data: Dict[str, Any]) -> str:
        """Generate comprehensive HTML report with all data sources"""
        company_name = merged_data['company_name']
        
        # Import GeminiProcessor for report generation
        try:
            from gemini_processor import GeminiProcessor
            processor = GeminiProcessor()
            
            # Generate report using existing processor but with merged data
            html_content = processor.generate_company_report(merged_data)
            
            if not html_content:
                html_content = self.create_fallback_comprehensive_html(merged_data)
                
        except Exception as e:
            print(f"[WARNING] Error with Gemini processor: {e}")
            html_content = self.create_fallback_comprehensive_html(merged_data)
        
        return html_content
    
    def create_fallback_comprehensive_html(self, merged_data: Dict[str, Any]) -> str:
        """Create fallback HTML report if Gemini fails"""
        company_name = merged_data['company_name']
        
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comprehensive Portfolio Update: {company_name}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 40px; background-color: #f8f9fa; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; text-align: center; }}
        .summary {{ background: #ecf0f1; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .sources {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin: 20px 0; }}
        .source-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; border-left: 4px solid #3498db; }}
        .sentiment-positive {{ color: #27ae60; font-weight: bold; }}
        .sentiment-negative {{ color: #e74c3c; font-weight: bold; }}
        .sentiment-neutral {{ color: #f39c12; font-weight: bold; }}
        .articles {{ margin-top: 30px; }}
        .article {{ background: #f8f9fa; margin: 10px 0; padding: 15px; border-radius: 8px; border-left: 4px solid #95a5a6; }}
        .article.linkedin {{ border-left-color: #0077b5; }}
        .article.gmail {{ border-left-color: #ea4335; }}
        .article.youtube {{ border-left-color: #ff0000; }}
        .article.reddit {{ border-left-color: #ff4500; }}
        .article.google_search {{ border-left-color: #4285f4; }}
        .article.blog {{ border-left-color: #2ecc71; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Comprehensive Portfolio Update: {company_name}</h1>
        
        <div class="summary">
            <h2>📊 Executive Summary</h2>
            <p><strong>Total Articles Analyzed:</strong> {merged_data['total_articles']}</p>
            <p><strong>Overall Sentiment:</strong> <span class="sentiment-{merged_data['overall_sentiment']}">{merged_data['overall_sentiment'].title()}</span></p>
            <p><strong>Analysis Date:</strong> {datetime.now().strftime('%B %d, %Y')}</p>
        </div>
        
        <div class="sources">
            <div class="source-card">
                <h3>📝 Blog Posts</h3>
                <p><strong>{merged_data['sources_count']['blog']}</strong> articles</p>
            </div>
            <div class="source-card">
                <h3>🔍 Google News</h3>
                <p><strong>{merged_data['sources_count']['google_search']}</strong> articles</p>
            </div>
            <div class="source-card">
                <h3>🔴 Reddit</h3>
                <p><strong>{merged_data['sources_count']['reddit']}</strong> posts</p>
            </div>
            <div class="source-card">
                <h3>📺 YouTube</h3>
                <p><strong>{merged_data['sources_count']['youtube']}</strong> videos</p>
            </div>
            <div class="source-card">
                <h3>🔗 LinkedIn</h3>
                <p><strong>{merged_data['sources_count']['linkedin']}</strong> activities</p>
            </div>
            <div class="source-card">
                <h3>📧 Gmail</h3>
                <p><strong>{merged_data['sources_count']['gmail']}</strong> messages</p>
            </div>
        </div>
        
        <div class="articles">
            <h2>📰 Recent Articles & Activities</h2>
            {self._generate_article_list(merged_data['articles'][:20])}
        </div>
        
        <div style="margin-top: 40px; text-align: center; color: #7f8c8d;">
            <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            <p>Comprehensive analysis across 6 data sources</p>
        </div>
    </div>
</body>
</html>"""
    
    def _generate_article_list(self, articles: List[Dict]) -> str:
        """Generate HTML for article list"""
        html = ""
        for article in articles:
            source_icon = {
                'blog': '📝',
                'google_search': '🔍', 
                'reddit': '🔴',
                'youtube': '📺',
                'linkedin': '🔗',
                'gmail': '📧'
            }.get(article['source'], '📄')
            
            html += f"""
            <div class="article {article['source']}">
                <h4>{source_icon} {article['headline']}</h4>
                <p>{article['description']}</p>
                <small>Source: {article['source'].replace('_', ' ').title()} | Date: {article['date']}</small>
            </div>
            """
        return html
    
    def process_company(self, company_name: str) -> str:
        """Process all data for a company and generate final report"""
        try:
            # Merge all data sources
            merged_data = self.merge_all_data(company_name)
            
            # Generate comprehensive HTML report
            print(f"\n[GENERATE] Generating comprehensive HTML report...")
            html_content = self.generate_comprehensive_html_report(merged_data)

            # Save final report
            output_filename = f"final_report_{company_name.replace(' ', '_').replace('.', '').lower()}.html"
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"[SUCCESS] Final comprehensive report saved to: {output_filename}")
            return output_filename
            
        except Exception as e:
            print(f"[ERROR] Error processing {company_name}: {e}")
            raise

def main():
    """Entry point"""
    if len(sys.argv) != 2:
        print("Usage: python merge_reports.py <company_name>")
        sys.exit(1)
    
    company_name = sys.argv[1]
    merger = ReportMerger()
    merger.process_company(company_name)

if __name__ == "__main__":
    main()
