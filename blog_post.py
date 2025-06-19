import requests
from bs4 import BeautifulSoup
import json
import time
import random
from config import GEMINI_API_KEY, GEMINI_API_URL, HEADERS


def call_gemini(prompt: str):
    response = requests.post(GEMINI_API_URL, json={
        "contents": [{"parts": [{"text": prompt}]}]
    })
    response.raise_for_status()
    return response.json()['candidates'][0]['content']['parts'][0]['text']


def find_official_blog_url(company_name: str) -> str:
    prompt = f"""yYou are a web researcher. Find the official blog, news, or press release page URL of the company "{company_name}". Return ONLY the URL. Example output:
https://stripe.com/blog
Now return only the official blog or news page URL for: {company_name}"""
    url = call_gemini(prompt).strip().split('\n')[0]
    return url


def extract_posts(blog_url: str):
    print(f"Scraping: {blog_url}")
    response = requests.get(blog_url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Collect all links on the page
    links = [a['href'] for a in soup.find_all('a', href=True)]
    full_links = list(set([
        link if link.startswith('http') else blog_url.rstrip('/') + '/' + link.lstrip('/')
        for link in links
    ]))

    # Filter likely article links
    valid_links = []
    for link in full_links:
        link = link.split('#')[0]  # Remove fragment
        if any(x in link.lower() for x in [
            '/about', '/contact', '/privacy', '/terms', '/careers',
            '/cookie', '/support', '/industries', '/resources',
            '/partners', '/newsroom', '/page/', '/tag/', '/faq', '/help'
        ]):
            continue
        # More flexible blog post detection
        if any(x in link.lower() for x in ['/blog/', '/news/', '/press/', '/article/', '/post/']):
            # Check if it looks like an article (has date or slug pattern)
            if any(x in link.lower() for x in ['-', '_', '/', '2024', '2023', '2022', '2021', '2020']):
                valid_links.append(link)

    # Remove duplicates
    valid_links = list(set(valid_links))

    posts = []
    for url in valid_links:
        try:
            post = extract_post_content(url)
            if post:
                posts.append(post)
        except Exception as e:
            print(f"Error scraping {url}: {e}")
        time.sleep(random.uniform(1, 2))  # polite scraping
    return posts


def extract_post_content(url: str):
    print(f"Fetching post: {url}")
    response = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    headline_tag = soup.find(['h1', 'title'])
    headline = headline_tag.get_text(strip=True) if headline_tag else "No Title"

    paragraphs = soup.find_all('p')
    content = '\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])

    # Try to extract date from <time> or fallback to LLM
    date_tag = soup.find('time')
    date = date_tag.get('datetime') if date_tag and date_tag.has_attr('datetime') else None
    if not date and date_tag:
        date = date_tag.get_text(strip=True)

    if not date:
        date = get_date_from_llm(headline, content, url)

    return {
        "headline": headline,
        "date": date,
        "content": content,
        "source_url": url
    }


def get_date_from_llm(title: str, content: str, url: str) -> str:
    print(f"Getting date from Gemini for {url}")
    prompt = f"""You are a news analyst. Given the following blog post, identify the **exact publication date** of the article. If not possible, return "Unknown".

Title: {title}

Content:
{content[:2000]}

URL: {url}

Return only the date in YYYY-MM-DD format. If not found, return "Unknown"."""
    try:
        return call_gemini(prompt).strip()
    except Exception as e:
        print(f"Gemini error for {url}: {e}")
        return "Unknown"


def run_pipeline(company_name: str, save_to_file=False):
    """
    Extract blog posts for a company
    Args:
        company_name: Name of the company
        save_to_file: Whether to save results to JSON file
    Returns:
        List of blog posts
    """
    print(f"Processing blog posts for: {company_name}")
    try:
        blog_url = find_official_blog_url(company_name)
        if not blog_url.startswith('http'):
            print(f"Warning: Invalid blog URL returned for {company_name}: {blog_url}")
            return []

        posts = extract_posts(blog_url)

        if save_to_file:
            filename = f"{company_name.replace(' ', '_')}_blog_posts.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(posts, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(posts)} posts to {filename}")

        print(f"Found {len(posts)} blog posts for {company_name}")
        return posts

    except Exception as e:
        print(f"Error processing blog posts for {company_name}: {e}")
        return []


def get_company_blog_data(company_name: str):
    """
    Main function to get blog data for a company
    Returns standardized data format
    """
    return run_pipeline(company_name, save_to_file=False)


if __name__ == "__main__":
    company = "Cart.com"
    posts = run_pipeline(company, save_to_file=True)
    print(json.dumps(posts[:2], indent=2, ensure_ascii=False))  # show first 2 posts
