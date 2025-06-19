import requests
import json
from datetime import datetime
from config import GEMINI_API_KEY, GEMINI_API_URL

class GeminiProcessor:
    def __init__(self):
        self.api_key = GEMINI_API_KEY
        self.api_url = GEMINI_API_URL

    def call_gemini(self, prompt):
        """Make API call to Gemini"""
        try:
            response = requests.post(self.api_url, json={
                "contents": [{"parts": [{"text": prompt}]}]
            })
            response.raise_for_status()
            html_content = response.json()['candidates'][0]['content']['parts'][0]['text']

            # Clean up malformed HTML responses from Gemini
            if html_content:
                # Remove markdown code blocks if present
                if '```html' in html_content:
                    # Extract content between ```html and ```
                    start_marker = '```html'
                    end_marker = '```'
                    start_idx = html_content.find(start_marker)
                    if start_idx != -1:
                        start_idx += len(start_marker)
                        end_idx = html_content.find(end_marker, start_idx)
                        if end_idx != -1:
                            html_content = html_content[start_idx:end_idx].strip()

                # Remove any remaining markdown artifacts
                html_content = html_content.replace('```html', '').replace('```', '').strip()

            return html_content
        except Exception as e:
            print(f"Error calling Gemini API: {e}")
            return None

    def create_portfolio_prompt(self, company_data):
        """Create the portfolio analysis prompt for a single company"""

        # Filter for positive/neutral articles as specified
        filtered_articles = [
            article for article in company_data['articles']
            if article['sentiment'] in ['positive', 'neutral']
        ]

        # If no positive/neutral articles, use all articles
        if not filtered_articles:
            filtered_articles = company_data['articles']

        # Limit articles to prevent token overflow (keep top 20 by sentiment score)
        filtered_articles = sorted(filtered_articles, key=lambda x: x['sentiment_score'], reverse=True)[:20]

        prompt = f"""You are "Portfolio Performance Analyst AI."

**Objective:**
Generate a concise, insightful, and actionable executive-level portfolio update email for venture investors, using the provided company-specific JSON news data. Prioritize positive or neutral sentiment articles for thematic synthesis.

**Audience:**
Sophisticated, time-constrained investors demanding clarity, precision, and strategic foresight.

**Input Data:**
Company: {company_data['company_name']}
Total Articles Analyzed: {company_data['total_articles']}
Overall Sentiment: {company_data['overall_sentiment']}
Sources: Blog ({company_data['sources_count']['blog']}), News ({company_data['sources_count']['google_search']}), Reddit ({company_data['sources_count']['reddit']}), YouTube ({company_data['sources_count']['youtube']}){', LinkedIn (' + str(company_data['sources_count'].get('linkedin', 0)) + ')' if company_data['sources_count'].get('linkedin', 0) > 0 else ''}{', Gmail (' + str(company_data['sources_count'].get('gmail', 0)) + ')' if company_data['sources_count'].get('gmail', 0) > 0 else ''}

JSON Articles: {json.dumps(filtered_articles, indent=2)}

**Communication Mandate: Top-Tier Consultant Standard (e.g., McKinsey, BCG, Bain)**
*   **Tone:** Executive presence—authoritative, confident, composed.
*   **Structure:** Top-down (key message first), MECE.
*   **Language:** Extremely concise. No fluff, hedging (e.g., "I think"), or casualisms.
*   **Focus:** Strategic insight, proactive analysis, forward-looking implications.

**Output: Single, Professionally Formatted HTML Email**

Generate a complete HTML document with the following structure:

1.  **Subject:** Portfolio Update: {company_data['company_name']} – Key Developments & Outlook ({datetime.now().strftime('%B %Y')})
2.  **Headline Lead:** (1 impactful sentence summarizing {company_data['company_name']}'s period.)
3.  **I. Executive Summary:**
    *   (Max 3 concise, impactful bullet points – Rule of 3. Synthesize *critical takeaways*.)
4.  **II. Key Developments & Themes:**
    *   (2-4 themes. For each: Clear Heading + *Brief, synthesized explanation from articles. Focus on *what happened* and its *immediate significance*. **No individual article summaries.**)
5.  **III. Overall Sentiment Analysis:**
    *   **Overall Sentiment:** {company_data['overall_sentiment'].title()}
    *   **Rationale:** (1-2 sentences justifying aggregated sentiment from news balance/impact.)
6.  **IV. Strategic Implications for Investors:**
    *   (2-3 forward-looking bullet points – The "So What?". Articulate what developments *mean for the investment*. Consultant-grade.)

**IMPORTANT:**
- Return ONLY the HTML content, no markdown
- Use professional styling with CSS
- Make it email-ready
- All analysis derived *solely* from provided JSON
- Focus on synthesis, not individual article summaries

**Constraint:** All analysis derived *solely* from provided JSON."""

        return prompt

    def generate_company_report(self, company_data):
        """Generate HTML report for a single company"""
        prompt = self.create_portfolio_prompt(company_data)
        html_content = self.call_gemini(prompt)

        if not html_content:
            return self.create_fallback_html(company_data)

        # Ensure it's proper HTML
        if not html_content.strip().startswith('<'):
            html_content = self.wrap_in_html(html_content, company_data['company_name'])

        return html_content

    def wrap_in_html(self, content, company_name):
        """Wrap content in proper HTML structure"""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Update: {company_name}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 40px; background-color: #f8f9fa; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        h3 {{ color: #7f8c8d; }}
        .executive-summary {{ background-color: #ecf0f1; padding: 20px; border-left: 4px solid #3498db; margin: 20px 0; }}
        .sentiment-positive {{ color: #27ae60; font-weight: bold; }}
        .sentiment-negative {{ color: #e74c3c; font-weight: bold; }}
        .sentiment-neutral {{ color: #f39c12; font-weight: bold; }}
        ul {{ padding-left: 20px; }}
        li {{ margin-bottom: 8px; }}
    </style>
</head>
<body>
    <div class="container">
        {content}
    </div>
</body>
</html>"""

    def create_fallback_html(self, company_data):
        """Create a fallback HTML report if Gemini fails"""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Update: {company_data['company_name']}</title>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; margin: 40px; }}
        .container {{ max-width: 800px; margin: 0 auto; }}
        h1 {{ color: #333; }}
        .error {{ color: #e74c3c; background: #fdf2f2; padding: 15px; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Portfolio Update: {company_data['company_name']}</h1>
        <div class="error">
            <p><strong>Report Generation Error</strong></p>
            <p>Unable to generate detailed analysis. Raw data summary:</p>
            <ul>
                <li>Total Articles: {company_data['total_articles']}</li>
                <li>Overall Sentiment: {company_data['overall_sentiment']}</li>
                <li>Sources: Blog ({company_data['sources_count']['blog']}), News ({company_data['sources_count']['google_search']}), Reddit ({company_data['sources_count']['reddit']}), YouTube ({company_data['sources_count']['youtube']}){', LinkedIn (' + str(company_data['sources_count'].get('linkedin', 0)) + ')' if company_data['sources_count'].get('linkedin', 0) > 0 else ''}{', Gmail (' + str(company_data['sources_count'].get('gmail', 0)) + ')' if company_data['sources_count'].get('gmail', 0) > 0 else ''}</li>
            </ul>
        </div>
    </div>
</body>
</html>"""

    def combine_company_reports(self, company_reports):
        """Combine multiple company reports into a single HTML document"""
        combined_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portfolio Update - Multiple Companies</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 40px; background-color: #f8f9fa; }
        .container { max-width: 1000px; margin: 0 auto; }
        .company-report { background: white; margin: 30px 0; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .company-separator { border-top: 3px solid #3498db; margin: 50px 0; }
        h1 { color: #2c3e50; text-align: center; margin-bottom: 40px; }
        .toc { background: #ecf0f1; padding: 20px; border-radius: 8px; margin-bottom: 40px; }
        .toc ul { list-style-type: none; padding: 0; }
        .toc li { margin: 10px 0; }
        .toc a { text-decoration: none; color: #3498db; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Portfolio Performance Update - Multi-Company Analysis</h1>
        <div class="toc">
            <h2>Table of Contents</h2>
            <ul>"""

        # Add table of contents
        for i, (company_name, _) in enumerate(company_reports.items()):
            combined_html += f'<li><a href="#company-{i}">{company_name}</a></li>'

        combined_html += """
            </ul>
        </div>"""

        # Add individual company reports
        for i, (company_name, report_html) in enumerate(company_reports.items()):
            # Extract body content from individual reports
            if '<body>' in report_html and '</body>' in report_html:
                body_content = report_html.split('<body>')[1].split('</body>')[0]
                # Remove container div if present
                if '<div class="container">' in body_content:
                    body_content = body_content.replace('<div class="container">', '').replace('</div>', '', 1)
            else:
                body_content = report_html

            combined_html += f"""
        <div class="company-report" id="company-{i}">
            {body_content}
        </div>
        {'' if i == len(company_reports) - 1 else '<div class="company-separator"></div>'}"""

        combined_html += """
    </div>
</body>
</html>"""

        return combined_html
