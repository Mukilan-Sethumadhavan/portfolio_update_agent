import praw
from datetime import datetime
from config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    username=REDDIT_USERNAME,
    password=REDDIT_PASSWORD,
    user_agent="Portfolio News Aggregator v1.0"
)

def search_reddit_posts(query, limit=10, return_data=False):
    """
    Search Reddit for posts about a query
    Args:
        query: Search query
        limit: Number of posts to return
        return_data: If True, return data instead of printing
    Returns:
        List of posts if return_data=True, None otherwise
    """
    print(f"Searching Reddit for posts about: {query}")

    posts_data = []

    try:
        for post in reddit.subreddit("all").search(query, sort="new", limit=limit):
            title = post.title
            content = post.selftext.strip() if post.selftext else "[No text content – possibly a link/image post]"
            date = datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S UTC')

            post_data = {
                'title': title,
                'content': content,
                'date': date,
                'permalink': post.permalink,
                'score': post.score,
                'num_comments': post.num_comments,
                'subreddit': str(post.subreddit)
            }

            posts_data.append(post_data)

            if not return_data:
                print(f"📌 Title: {title}")
                print(f"🕒 Date Posted: {date}")
                print(f"📝 Content:\n{content}")
                print("-" * 80)

    except Exception as e:
        print(f"Error searching Reddit for {query}: {e}")

    if return_data:
        return posts_data


def get_company_reddit_data(company_name: str, limit=20):
    """
    Main function to get Reddit data for a company
    Returns standardized data format
    """
    print(f"Processing Reddit posts for: {company_name}")
    posts = search_reddit_posts(company_name, limit=limit, return_data=True)
    print(f"Found {len(posts)} Reddit posts for {company_name}")
    return posts


if __name__ == "__main__":
    # Example usage
    search_reddit_posts("anthropic", limit=5)  # Change topic as needed
