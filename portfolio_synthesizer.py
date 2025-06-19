#!/usr/bin/env python3
"""
Portfolio Synthesizer - Creates merged analysis from multiple company reports

This module takes the final results from all analyzed companies and feeds them to Gemini
to create a synthesized portfolio overview that analyzes the companies collectively.
"""

import json
from datetime import datetime
from typing import Dict, List, Any
from gemini_processor import GeminiProcessor

class PortfolioSynthesizer:
    def __init__(self):
        self.gemini_processor = GeminiProcessor()

    def create_portfolio_synthesis_prompt(self, companies_data: Dict[str, Dict[str, Any]]) -> str:
        """Create a prompt for Gemini to synthesize portfolio-level insights"""

        # Prepare company summaries for Gemini
        company_summaries = []
        total_articles = 0
        overall_sentiments = []

        for company_name, data in companies_data.items():
            total_articles += data['total_articles']
            overall_sentiments.append(data['overall_sentiment'])

            company_summary = {
                'company': company_name,
                'total_articles': data['total_articles'],
                'overall_sentiment': data['overall_sentiment'],
                'sentiment_score': data['overall_sentiment_score'],
                'sources_breakdown': data['sources_count'],
                'key_articles': data['articles'][:5]  # Top 5 articles for context
            }
            company_summaries.append(company_summary)

        # Calculate portfolio-level metrics
        portfolio_sentiment_score = sum(data['overall_sentiment_score'] for data in companies_data.values()) / len(companies_data)
        portfolio_sentiment = 'positive' if portfolio_sentiment_score >= 0.05 else 'negative' if portfolio_sentiment_score <= -0.05 else 'neutral'

        # Adjust prompt based on single vs multiple companies
        if len(companies_data) == 1:
            company_name = list(companies_data.keys())[0]
            analysis_type = "STRATEGIC INVESTMENT ANALYSIS"
            focus_description = f"deep strategic analysis of {company_name} as an investment opportunity"
            structure_note = "Focus on strategic investment implications, market positioning, and growth potential"
        else:
            analysis_type = "PORTFOLIO SYNTHESIS"
            focus_description = f"portfolio-level insights analyzing {len(companies_data)} companies collectively as an investment portfolio"
            structure_note = "Focus on portfolio-level insights, cross-company synergies, and strategic themes"

        prompt = f"""You are "Portfolio Performance Analyst AI" - Senior Partner Level.

**Objective:**
Generate a comprehensive, executive-level {analysis_type} that provides {focus_description}. This is strategic, forward-looking analysis for sophisticated venture investors.

**Investment Data:**
Companies Analyzed: {', '.join(companies_data.keys())}
Total Articles Processed: {total_articles}
Overall Sentiment Score: {portfolio_sentiment_score:.3f}
Overall Sentiment: {portfolio_sentiment}

**Company Data:**
{json.dumps(company_summaries, indent=2)}

**Communication Mandate: Senior Partner/Managing Director Standard**
*   **Tone:** Senior executive presence—strategic, authoritative, forward-looking
*   **Structure:** {structure_note}
*   **Language:** Sophisticated investment terminology
*   **Focus:** Strategic insights, risk/return optimization, actionable recommendations

**Output: Complete HTML Strategic Analysis Report**

Generate a complete, professionally styled HTML document with the following structure:

1.  **Executive Summary:**
    *   (Strategic headline - one powerful sentence about the investment thesis)
    *   (3 strategic insights that emerge from the analysis)

2.  **Strategic Analysis:**
    *   **Market Positioning & Competitive Dynamics:** (Market opportunity and competitive landscape analysis)
    *   **Technology & Innovation Assessment:** (Technology advantages and innovation potential)
    *   **Risk & Opportunity Evaluation:** (Risk factors and growth opportunities)

3.  **Sentiment & Market Dynamics:**
    *   **Market Sentiment:** {portfolio_sentiment.title()}
    *   **Sentiment Analysis:** (What market sentiment indicates about investment timing and potential)
    *   **Market Timing Considerations:** (Strategic timing insights from news cycle analysis)

4.  **Strategic Investment Implications:**
    *   **Investment Thesis:** (Core investment rationale and value creation potential)
    *   **Risk-Return Profile:** (Risk assessment and return potential analysis)
    *   **Strategic Recommendations:** (Actionable investment strategy recommendations)

5.  **Investment Committee Recommendations:**
    *   (2-3 specific, actionable recommendations for investment decisions)
    *   (Strategic focus areas and next steps)

**CRITICAL REQUIREMENTS:**
- Return ONLY complete HTML with professional CSS styling
- Provide strategic, investment-focused analysis
- Use sophisticated investment terminology
- Include actionable recommendations
- Make it suitable for senior investment committee presentation

**HTML Styling Requirements:**
- Professional executive presentation styling
- Color-coded sentiment indicators
- Clean, sophisticated layout
- Responsive design
- Print-friendly formatting

**Constraint:** All analysis derived *solely* from provided data."""

        return prompt

    def generate_portfolio_synthesis(self, companies_data: Dict[str, Dict[str, Any]]) -> str:
        """Generate a synthesized portfolio analysis HTML report"""
        print(f"\n🔄 Generating portfolio synthesis for {len(companies_data)} companies...")

        prompt = self.create_portfolio_synthesis_prompt(companies_data)
        html_content = self.gemini_processor.call_gemini(prompt)

        if not html_content:
            return self.create_fallback_portfolio_synthesis(companies_data)

        # Ensure it's proper HTML (the call_gemini method now handles markdown cleanup)
        if not html_content.strip().startswith('<!DOCTYPE html>') and not html_content.strip().startswith('<html'):
            html_content = self.wrap_portfolio_synthesis_in_html(html_content, companies_data)

        return html_content

    def wrap_portfolio_synthesis_in_html(self, content: str, companies_data: Dict[str, Dict[str, Any]]) -> str:
        """Wrap content in proper HTML structure for portfolio synthesis"""
        company_names = ', '.join(companies_data.keys())
        total_articles = sum(data['total_articles'] for data in companies_data.values())

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Synthesis: {company_names}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 40px; background-color: #f8f9fa; }}
        .container {{ max-width: 900px; margin: 0 auto; background: white; padding: 50px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #1a365d; border-bottom: 4px solid #3182ce; padding-bottom: 15px; text-align: center; }}
        h2 {{ color: #2d3748; margin-top: 40px; border-left: 5px solid #3182ce; padding-left: 15px; }}
        h3 {{ color: #4a5568; }}
        .portfolio-summary {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin: 30px 0; }}
        .thematic-analysis {{ background-color: #f7fafc; padding: 25px; border-left: 5px solid #38b2ac; margin: 25px 0; }}
        .sentiment-positive {{ color: #38a169; font-weight: bold; }}
        .sentiment-negative {{ color: #e53e3e; font-weight: bold; }}
        .sentiment-neutral {{ color: #d69e2e; font-weight: bold; }}
        .recommendations {{ background-color: #fffaf0; padding: 25px; border: 2px solid #ed8936; border-radius: 8px; margin: 25px 0; }}
        ul {{ padding-left: 25px; }}
        li {{ margin-bottom: 12px; }}
        .portfolio-metrics {{ display: flex; justify-content: space-around; background: #edf2f7; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .metric {{ text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #3182ce; }}
        .metric-label {{ font-size: 14px; color: #718096; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Portfolio Synthesis Report</h1>
        <div class="portfolio-metrics">
            <div class="metric">
                <div class="metric-value">{len(companies_data)}</div>
                <div class="metric-label">Companies</div>
            </div>
            <div class="metric">
                <div class="metric-value">{total_articles}</div>
                <div class="metric-label">Articles Analyzed</div>
            </div>
            <div class="metric">
                <div class="metric-value">{datetime.now().strftime('%b %Y')}</div>
                <div class="metric-label">Analysis Period</div>
            </div>
        </div>
        {content}
    </div>
</body>
</html>"""

    def create_fallback_portfolio_synthesis(self, companies_data: Dict[str, Dict[str, Any]]) -> str:
        """Create a fallback portfolio synthesis if Gemini fails"""
        company_names = ', '.join(companies_data.keys())
        total_articles = sum(data['total_articles'] for data in companies_data.values())
        avg_sentiment_score = sum(data['overall_sentiment_score'] for data in companies_data.values()) / len(companies_data)
        portfolio_sentiment = 'positive' if avg_sentiment_score >= 0.05 else 'negative' if avg_sentiment_score <= -0.05 else 'neutral'

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Synthesis: {company_names}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 40px; background-color: #f8f9fa; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; text-align: center; }}
        .error {{ color: #e74c3c; background: #fdf2f2; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        .portfolio-summary {{ background: #ecf0f1; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        .sentiment-positive {{ color: #27ae60; font-weight: bold; }}
        .sentiment-negative {{ color: #e74c3c; font-weight: bold; }}
        .sentiment-neutral {{ color: #f39c12; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Portfolio Synthesis Report</h1>
        <div class="error">
            <p><strong>Portfolio Synthesis Generation Error</strong></p>
            <p>Unable to generate detailed portfolio analysis. Portfolio summary:</p>
        </div>
        <div class="portfolio-summary">
            <h2>Portfolio Overview</h2>
            <ul>
                <li><strong>Companies Analyzed:</strong> {company_names}</li>
                <li><strong>Total Articles Processed:</strong> {total_articles}</li>
                <li><strong>Portfolio Sentiment:</strong> <span class="sentiment-{portfolio_sentiment}">{portfolio_sentiment.title()}</span></li>
                <li><strong>Average Sentiment Score:</strong> {avg_sentiment_score:.3f}</li>
            </ul>
            <h3>Individual Company Breakdown:</h3>
            <ul>
                {''.join([f'<li><strong>{name}:</strong> {data["total_articles"]} articles, {data["overall_sentiment"]} sentiment</li>' for name, data in companies_data.items()])}
            </ul>
        </div>
    </div>
</body>
</html>"""

    def save_portfolio_synthesis(self, companies_data: Dict[str, Dict[str, Any]], output_filename: str = None) -> str:
        """Generate and save portfolio synthesis report"""
        if not output_filename:
            company_names = '_'.join([name.lower().replace(' ', '').replace('.', '') for name in companies_data.keys()])
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Different naming for single vs multiple companies
            if len(companies_data) == 1:
                output_filename = f"strategic_analysis_{company_names}_{timestamp}.html"
            else:
                output_filename = f"portfolio_synthesis_{company_names}_{timestamp}.html"

        html_content = self.generate_portfolio_synthesis(companies_data)

        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

        analysis_type = "Strategic analysis" if len(companies_data) == 1 else "Portfolio synthesis"
        print(f"✅ {analysis_type} saved to: {output_filename}")
        return output_filename


def create_portfolio_synthesis_from_files(aggregated_files: List[str]) -> str:
    """Create portfolio synthesis from aggregated JSON files"""
    synthesizer = PortfolioSynthesizer()
    companies_data = {}

    for file_path in aggregated_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                companies_data[data['company_name']] = data
        except Exception as e:
            print(f"Error loading {file_path}: {e}")

    if not companies_data:
        print("No valid company data found")
        return None

    return synthesizer.save_portfolio_synthesis(companies_data)


if __name__ == "__main__":
    # Example usage with the files we just generated
    aggregated_files = [
        "aggregated_anthropic.json",
        "aggregated_tesla.json",
        "aggregated_neuralink.json"
    ]

    output_file = create_portfolio_synthesis_from_files(aggregated_files)
    if output_file:
        print(f"Portfolio synthesis complete: {output_file}")
    else:
        print("Failed to create portfolio synthesis")
