import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
from config import GOOGLE_SEARCH_API_KEY, GOOGLE_CSE_ID, DEFAULT_DATE_RANGE_DAYS

def google_search(query, start_date, end_date, api_key, cse_id, num=10):
    """
    Perform a Google Custom Search for the given query and date range.
    """
    search_url = 'https://www.googleapis.com/customsearch/v1'
    results = []
    start = 1

    while start < num:
        params = {
            'q': query,
            'cx': cse_id,
            'key': api_key,
            'num': min(10, num - start + 1),
            'start': start,
            'sort': f'date:r:{start_date}:{end_date}',
        }
        response = requests.get(search_url, params=params)
        data = response.json()
        items = data.get('items', [])
        results.extend(items)
        start += 10
        if 'nextPage' not in data.get('queries', {}):
            break
    return results

def extract_article_details(url):
    """
    Fetch and parse the article content from the given URL.
    """
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Attempt to extract the publication date
        date = None
        for meta in soup.find_all('meta'):
            if meta.get('property') in ['article:published_time', 'og:pubdate', 'pubdate']:
                date = meta.get('content')
                break
        if not date:
            # Fallback: use current date
            date = datetime.now().isoformat() + 'Z'

        # Extract headline
        headline = soup.title.string if soup.title else ''

        # Extract description
        description = ''
        desc_meta = soup.find('meta', attrs={'name': 'description'})
        if desc_meta:
            description = desc_meta.get('content', '')

        # Extract full content
        paragraphs = soup.find_all('p')
        full_content = '\n'.join([p.get_text() for p in paragraphs])

        # Extract image URL
        image_url = ''
        og_image = soup.find('meta', property='og:image')
        if og_image:
            image_url = og_image.get('content', '')

        return {
            'date': date,
            'headline': headline,
            'description': description,
            'url': url,
            'image_url': image_url,
            'full_content': full_content
        }
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def fetch_company_news(company_name, start_date_str=None, end_date_str=None):
    """
    Fetch news articles for the specified company within the date range.
    """
    # Set default date range if not provided
    if not start_date_str or not end_date_str:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')

    articles = []
    try:
        search_results = google_search(f"{company_name} news", start_date_str, end_date_str, GOOGLE_SEARCH_API_KEY, GOOGLE_CSE_ID, num=20)
        for item in search_results:
            url = item.get('link')
            article = extract_article_details(url)
            if article:
                articles.append(article)
    except Exception as e:
        print(f"Error fetching Google search results for {company_name}: {e}")

    return articles


def get_company_google_data(company_name: str):
    """
    Main function to get Google search data for a company
    Returns standardized data format
    """
    print(f"Processing Google search results for: {company_name}")
    articles = fetch_company_news(company_name)
    print(f"Found {len(articles)} Google search articles for {company_name}")
    return articles


if __name__ == "__main__":
    # Define the company name and date range
    company_name = "Cart.com"
    start_date = datetime(2025, 1, 16)
    end_date = datetime(2025, 1, 23)

    # Format dates as YYYYMMDD
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')

    # Fetch news articles
    news_articles = fetch_company_news(company_name, start_date_str, end_date_str)

    # Output the results in JSON format
    print(json.dumps(news_articles, indent=4))
