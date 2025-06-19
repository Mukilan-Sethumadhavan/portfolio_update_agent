# News Summarizer & Portfolio Intelligence System

A comprehensive AI-powered news aggregation and portfolio analysis system that collects data from multiple sources and generates executive-level reports using Google's Gemini AI.

## 🚀 Features

### Multi-Source Data Collection
- **Blog Posts**: Scrapes official company blogs and news pages
- **Google Search**: Custom search API integration for news articles
- **Reddit**: Community discussions and sentiment analysis
- **YouTube**: Video content with transcript extraction
- **LinkedIn**: Company activity and employee updates
- **Gmail**: Email content analysis (optional)

### AI-Powered Analysis
- **Sentiment Analysis**: VADER sentiment scoring for all content
- **Gemini AI Integration**: Executive-level report generation
- **Portfolio Synthesis**: Multi-company analysis and comparison
- **Strategic Insights**: Consultant-grade analysis and recommendations

### Advanced Features
- **GCP Integration**: Vertex AI for embeddings and matching engine
- **Report Deduplication**: Intelligent content deduplication
- **Cloud Storage**: Google Cloud Storage for report management
- **Market Intelligence Pipeline**: Automated analysis workflows

## 📋 Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account (for advanced features)
- API keys for various services (see Configuration section)

## 🛠 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd news-summarizer9
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up configuration**
   - Copy `config.py` and update with your API keys
   - Set up GCP credentials if using advanced features

## ⚙️ Configuration

### Required API Keys

Create a `config.py` file with the following API keys:

```python
# Gemini API Configuration
GEMINI_API_KEY = 'your_gemini_api_key'
GEMINI_API_URL = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}'

# Google Search API Configuration
GOOGLE_SEARCH_API_KEY = "your_google_search_api_key"
GOOGLE_CSE_ID = "your_custom_search_engine_id"

# YouTube API Configuration
YOUTUBE_API_KEY = 'your_youtube_api_key'

# Reddit API Configuration
REDDIT_CLIENT_ID = "your_reddit_client_id"
REDDIT_CLIENT_SECRET = "your_reddit_client_secret"
REDDIT_USERNAME = "your_reddit_username"
REDDIT_PASSWORD = "your_reddit_password"

# News API Configuration
NEWS_API_KEY = 'your_news_api_key'
```

### GCP Setup (Optional)

For advanced features, set up Google Cloud Platform:

1. Create a GCP project
2. Enable required APIs:
   - Vertex AI API
   - Cloud Storage API
   - Cloud Build API
3. Create service account and download credentials
4. Update `gcp_project_config.py` with your project details

## 🎯 Usage

### Basic Usage

Run the main application:
```bash
python main.py
```

The system will prompt you to enter company names and generate comprehensive reports.

### Advanced Usage

#### Market Intelligence Pipeline
```bash
python market_intelligence_pipeline.py
```

#### Individual Data Sources
```bash
# Blog posts only
python blog_post.py

# Google search results
python google_search.py

# Reddit posts
python reddit_scrap.py

# YouTube videos
python enhanced_youtube_scraping.py

# LinkedIn activity
python linkedin_scraper.py
```

#### Report Management
```bash
# Merge multiple reports
python merge_reports.py

# Process and deduplicate reports
python process_reports.py
```

## 📊 Output

The system generates several types of reports:

### Individual Company Reports
- `portfolio_report_[company].html`: Detailed company analysis
- `final_report_[company].html`: Executive summary
- `strategic_analysis_[company]_[timestamp].html`: Strategic insights

### Aggregated Data
- `aggregated_[company].json`: Raw collected data
- `linkedin_[company].json`: LinkedIn-specific data
- `gmail_[company].json`: Gmail-specific data

### Portfolio Reports
- Combined analysis of multiple companies
- Comparative insights and recommendations
- Executive-level summaries

## 🏗 Project Structure

```
news-summarizer9/
├── main.py                          # Main application entry point
├── config.py                        # Configuration and API keys
├── requirements.txt                 # Python dependencies
├── README.md                       # This file
│
├── Data Collection Modules/
│   ├── blog_post.py                # Blog scraping
│   ├── google_search.py            # Google search API
│   ├── reddit_scrap.py             # Reddit API integration
│   ├── enhanced_youtube_scraping.py # YouTube video analysis
│   ├── linkedin_scraper.py         # LinkedIn activity scraping
│   └── gm.py                       # Gmail integration
│
├── AI Processing/
│   ├── gemini_processor.py         # Gemini AI integration
│   ├── data_aggregator.py          # Data aggregation and sentiment analysis
│   └── portfolio_synthesizer.py    # Portfolio-level synthesis
│
├── GCP Integration/
│   ├── gcs_storage_manager.py      # Google Cloud Storage
│   ├── vertex_ai_embedding_service.py # Vertex AI embeddings
│   ├── vertex_ai_matching_engine.py # Matching engine
│   └── gcp_project_config.py       # GCP configuration
│
├── Report Management/
│   ├── merge_reports.py            # Report merging
│   ├── process_reports.py          # Report processing
│   ├── report_deduplication_manager.py # Deduplication
│   └── master_controller.py        # Orchestration
│
└── Generated Reports/
    ├── *.html                      # HTML reports
    ├── *.json                      # JSON data files
    └── phantom_results/            # LinkedIn scraping results
```

## 🔧 Customization

### Adding New Data Sources

1. Create a new Python module following the naming convention
2. Implement a `get_company_[source]_data(company_name)` function
3. Add the source to `data_aggregator.py`
4. Update the main workflow in `main.py`

### Modifying Report Templates

Edit the prompts in `gemini_processor.py` to customize:
- Report structure and format
- Analysis depth and focus
- Executive summary style
- Strategic recommendations

### Sentiment Analysis

The system uses VADER sentiment analysis. You can:
- Modify thresholds in `data_aggregator.py`
- Add custom sentiment analysis logic
- Integrate alternative sentiment analysis services

## 🚨 Troubleshooting

### Common Issues

1. **API Rate Limits**
   - Implement delays between requests
   - Use API key rotation
   - Monitor usage quotas

2. **Missing Dependencies**
   ```bash
   pip install -r requirements.txt --upgrade
   ```

3. **GCP Authentication**
   - Verify service account credentials
   - Check project permissions
   - Ensure APIs are enabled

4. **LinkedIn Scraping Issues**
   - Verify Phantom Buster API key
   - Check Google Sheets permissions
   - Monitor scraping quotas

### Debug Mode

Enable detailed logging by modifying the logging configuration in individual modules.

## 📈 Performance Optimization

### For Large-Scale Analysis
- Use GCP Vertex AI for parallel processing
- Implement caching for API responses
- Use async/await for concurrent requests
- Optimize database queries (if using)

### Memory Management
- Process companies in batches
- Clean up temporary files
- Use generators for large datasets

## 🔒 Security Considerations

- Store API keys securely (use environment variables)
- Implement rate limiting
- Validate all inputs
- Use HTTPS for all API calls
- Regularly rotate credentials

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review existing issues
3. Create a new issue with detailed information

## 🔄 Updates

Stay updated with the latest features and improvements by:
- Following the repository
- Checking the releases page
- Reviewing the changelog

---

**Note**: This system is designed for research and analysis purposes. Please ensure compliance with all applicable terms of service and data protection regulations when using third-party APIs and services.
