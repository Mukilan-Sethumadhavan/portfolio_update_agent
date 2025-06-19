import json
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class DataAggregator:
    def __init__(self):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of given text"""
        if not text:
            return 'neutral', 0.0
        
        score = self.sentiment_analyzer.polarity_scores(text)['compound']
        sentiment = 'positive' if score >= 0.05 else 'negative' if score <= -0.05 else 'neutral'
        return sentiment, score
    
    def standardize_blog_data(self, blog_data):
        """Convert blog post data to standard format"""
        standardized = []
        for post in blog_data:
            content = f"{post.get('headline', '')} {post.get('content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': post.get('headline', ''),
                'description': post.get('content', '')[:200] + '...' if len(post.get('content', '')) > 200 else post.get('content', ''),
                'url': post.get('source_url', ''),
                'image_url': '',
                'full_content': post.get('content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"Blog post: {post.get('headline', '')}",
                'source': 'blog',
                'date': post.get('date', 'Unknown')
            })
        return standardized
    
    def standardize_google_data(self, google_data):
        """Convert Google search data to standard format"""
        standardized = []
        for article in google_data:
            content = f"{article.get('headline', '')} {article.get('description', '')} {article.get('full_content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': article.get('headline', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'image_url': article.get('image_url', ''),
                'full_content': article.get('full_content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"News article: {article.get('headline', '')}",
                'source': 'google_search',
                'date': article.get('date', 'Unknown')
            })
        return standardized
    
    def standardize_reddit_data(self, reddit_data):
        """Convert Reddit data to standard format"""
        standardized = []
        for post in reddit_data:
            content = f"{post.get('title', '')} {post.get('content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': post.get('title', ''),
                'description': post.get('content', '')[:200] + '...' if len(post.get('content', '')) > 200 else post.get('content', ''),
                'url': f"https://reddit.com{post.get('permalink', '')}" if post.get('permalink') else '',
                'image_url': '',
                'full_content': post.get('content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"Reddit post: {post.get('title', '')}",
                'source': 'reddit',
                'date': post.get('date', 'Unknown')
            })
        return standardized
    
    def standardize_youtube_data(self, youtube_data):
        """Convert YouTube data to standard format"""
        standardized = []
        for video in youtube_data:
            content = f"{video.get('title', '')} {video.get('transcript', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': video.get('title', ''),
                'description': video.get('transcript', '')[:200] + '...' if len(video.get('transcript', '')) > 200 else video.get('transcript', ''),
                'url': video.get('url', ''),
                'image_url': '',
                'full_content': video.get('transcript', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"YouTube video: {video.get('title', '')}",
                'source': 'youtube',
                'date': video.get('published', 'Unknown')
            })
        return standardized

    def standardize_linkedin_data(self, linkedin_data):
        """Convert LinkedIn data to standard format"""
        standardized = []
        for post in linkedin_data:
            content = f"{post.get('headline', '')} {post.get('full_content', '')}"
            sentiment, sentiment_score = self.analyze_sentiment(content)
            
            standardized.append({
                'headline': post.get('headline', ''),
                'description': post.get('description', ''),
                'url': post.get('url', ''),
                'image_url': post.get('image_url', ''),
                'full_content': post.get('full_content', ''),
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'summary': f"LinkedIn activity: {post.get('headline', '')}",
                'source': 'linkedin',
                'date': post.get('date', 'Unknown')
            })
        return standardized

    def aggregate_company_data(self, company_name, blog_data=None, google_data=None, reddit_data=None, youtube_data=None, linkedin_data=None):
        """Aggregate all data sources for a company"""
        all_articles = []
        
        if blog_data:
            all_articles.extend(self.standardize_blog_data(blog_data))
        
        if google_data:
            all_articles.extend(self.standardize_google_data(google_data))
        
        if reddit_data:
            all_articles.extend(self.standardize_reddit_data(reddit_data))
        
        if youtube_data:
            all_articles.extend(self.standardize_youtube_data(youtube_data))

        if linkedin_data:
            all_articles.extend(self.standardize_linkedin_data(linkedin_data))

        # Calculate overall sentiment
        if all_articles:
            avg_sentiment_score = sum(article['sentiment_score'] for article in all_articles) / len(all_articles)
            overall_sentiment = 'positive' if avg_sentiment_score >= 0.05 else 'negative' if avg_sentiment_score <= -0.05 else 'neutral'
        else:
            overall_sentiment = 'neutral'
            avg_sentiment_score = 0.0
        
        return {
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
                'linkedin': len([a for a in all_articles if a['source'] == 'linkedin'])
            }
        }
    
    def save_aggregated_data(self, aggregated_data, filename):
        """Save aggregated data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(aggregated_data, f, indent=2, ensure_ascii=False)
        print(f"Aggregated data saved to {filename}")
