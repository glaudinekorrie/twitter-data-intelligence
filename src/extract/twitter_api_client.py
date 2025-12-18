"""
Twitter API Client for extracting tweet data
Handles authentication, rate limiting, and data extraction
"""

import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import time

# Try to import tweepy, but provide mock if not available
try:
    import tweepy
    TWEEP_AVAILABLE = True
except ImportError:
    TWEEP_AVAILABLE = False
    logging.warning("tweepy not available, using mock data")

# Try to import dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


class TwitterAPIClient:
    """Client for interacting with Twitter API"""
    
    def __init__(self, use_mock: bool = False):
        """
        Initialize Twitter API client
        
        Args:
            use_mock: If True, use mock data instead of real API calls
                     (Useful for development and testing)
        """
        self.use_mock = use_mock or not TWEEP_AVAILABLE
        
        if not self.use_mock:
            self._setup_api_client()
        else:
            logger.info("Using mock Twitter data for development")
    
    def _setup_api_client(self) -> None:
        """Set up Twitter API authentication"""
        try:
            # Get credentials from environment variables
            api_key = os.getenv("TWITTER_API_KEY")
            api_secret = os.getenv("TWITTER_API_SECRET")
            access_token = os.getenv("TWITTER_ACCESS_TOKEN")
            access_secret = os.getenv("TWITTER_ACCESS_SECRET")
            
            if not all([api_key, api_secret, access_token, access_secret]):
                logger.warning("Twitter API credentials not found. Using mock data.")
                self.use_mock = True
                return
            
            # Authenticate
            auth = tweepy.OAuthHandler(api_key, api_secret)
            auth.set_access_token(access_token, access_secret)
            
            # Create API client with rate limit handling
            self.api = tweepy.API(
                auth, 
                wait_on_rate_limit=True,
                wait_on_rate_limit_notify=True
            )
            
            logger.info("Twitter API client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Twitter API: {e}")
            self.use_mock = True
    
    def search_tweets(
        self, 
        query: str, 
        count: int = 100,
        since: Optional[str] = None,
        until: Optional[str] = None,
        lang: str = "en"
    ) -> List[Dict[str, Any]]:
        """
        Search for tweets matching a query
        
        Args:
            query: Search query string
            count: Number of tweets to return (max 100 per request)
            since: Start date (YYYY-MM-DD)
            until: End date (YYYY-MM-DD)
            lang: Language code
            
        Returns:
            List of tweet dictionaries
        """
        if self.use_mock:
            return self._get_mock_tweets(count)
        
        try:
            tweets = []
            
            # Build search parameters
            search_args = {
                "q": query,
                "count": min(count, 100),  # Twitter limit
                "lang": lang,
                "tweet_mode": "extended"
            }
            
            if since:
                search_args["since"] = since
            if until:
                search_args["until"] = until
            
            # Make API call
            logger.info(f"Searching Twitter for: {query}")
            search_results = self.api.search_tweets(**search_args)
            
            # Convert to dictionary format
            for tweet in search_results:
                tweet_dict = self._tweet_to_dict(tweet)
                tweets.append(tweet_dict)
            
            logger.info(f"Found {len(tweets)} tweets for query: {query}")
            return tweets
            
        except Exception as e:
            logger.error(f"Error searching tweets: {e}")
            return []
    
    def get_user_tweets(
        self, 
        username: str, 
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent tweets from a specific user"""
        if self.use_mock:
            return self._get_mock_tweets(count, username=username)
        
        try:
            tweets = []
            user_tweets = self.api.user_timeline(
                screen_name=username,
                count=count,
                tweet_mode="extended"
            )
            
            for tweet in user_tweets:
                tweet_dict = self._tweet_to_dict(tweet)
                tweets.append(tweet_dict)
            
            return tweets
            
        except Exception as e:
            logger.error(f"Error getting user tweets: {e}")
            return []
    
    def _tweet_to_dict(self, tweet) -> Dict[str, Any]:
        """Convert tweepy Tweet object to dictionary"""
        return {
            "tweet_id": str(tweet.id),
            "created_at": tweet.created_at,
            "text": tweet.full_text,
            "user_id": str(tweet.user.id),
            "user_name": tweet.user.screen_name,
            "user_display_name": tweet.user.name,
            "user_followers": tweet.user.followers_count,
            "user_following": tweet.user.friends_count,
            "retweet_count": tweet.retweet_count,
            "favorite_count": tweet.favorite_count,
            "reply_count": tweet.reply_count if hasattr(tweet, 'reply_count') else 0,
            "is_retweet": hasattr(tweet, 'retweeted_status'),
            "hashtags": [hashtag["text"] for hashtag in tweet.entities.get("hashtags", [])],
            "mentions": [mention["screen_name"] for mention in tweet.entities.get("user_mentions", [])],
            "urls": [url["expanded_url"] for url in tweet.entities.get("urls", [])],
            "language": tweet.lang if hasattr(tweet, 'lang') else "en",
            "source": tweet.source,
            "collected_at": datetime.now()
        }
    
    def _get_mock_tweets(self, count: int = 10, username: str = None) -> List[Dict[str, Any]]:
        """Generate mock tweet data for development"""
        import random
        from faker import Faker
        
        fake = Faker()
        tweets = []
        
        brands = ["Tesla", "Netflix", "Starbucks", "Apple", "Google"]
        sentiments = ["positive", "negative", "neutral"]
        
        for i in range(count):
            brand = random.choice(brands)
            sentiment = random.choice(sentiments)
            
            # Generate appropriate text based on sentiment
            if sentiment == "positive":
                texts = [
                    f"Loving my new {brand} product! Amazing experience!",
                    f"{brand} is changing the game! So impressed!",
                    f"Best service from {brand} ever! Highly recommend!"
                ]
            elif sentiment == "negative":
                texts = [
                    f"Really disappointed with {brand}. Terrible experience.",
                    f"{brand} customer service is awful. Never again!",
                    f"Worst purchase ever from {brand}. Stay away!"
                ]
            else:
                texts = [
                    f"Just bought a {brand} product. We'll see how it goes.",
                    f"Reading about {brand}'s new features. Interesting.",
                    f"Saw {brand} mentioned in the news today."
                ]
            
            tweet = {
                "tweet_id": f"mock_{random.randint(100000, 999999)}",
                "created_at": fake.date_time_this_month(),
                "text": random.choice(texts),
                "user_id": f"user_{random.randint(1000, 9999)}",
                "user_name": username or fake.user_name(),
                "user_display_name": fake.name(),
                "user_followers": random.randint(100, 1000000),
                "user_following": random.randint(10, 5000),
                "retweet_count": random.randint(0, 1000),
                "favorite_count": random.randint(0, 5000),
                "reply_count": random.randint(0, 100),
                "is_retweet": random.random() > 0.7,
                "hashtags": random.sample([brand.lower(), "tech", "review", "customer"], random.randint(0, 3)),
                "mentions": [f"@{fake.user_name()}" for _ in range(random.randint(0, 2))],
                "urls": [],
                "language": "en",
                "source": random.choice(["Twitter Web App", "Twitter for iPhone", "TweetDeck"]),
                "collected_at": datetime.now(),
                "brand_mentioned": brand,
                "sentiment": sentiment
            }
            
            tweets.append(tweet)
        
        return tweets
    
    def test_connection(self) -> bool:
        """Test if API connection works"""
        if self.use_mock:
            logger.info("Using mock data - connection test skipped")
            return True
        
        try:
            # Try to get rate limit status
            rate_limit_status = self.api.rate_limit_status()
            logger.info("Twitter API connection successful")
            logger.info(f"Rate limits: {rate_limit_status['resources']['search']}")
            return True
        except Exception as e:
            logger.error(f"Twitter API connection failed: {e}")
            return False


# Utility function for easy access
def get_twitter_client(use_mock: bool = False) -> TwitterAPIClient:
    """
    Factory function to get Twitter API client
    
    Args:
        use_mock: If True, returns client with mock data
        
    Returns:
        TwitterAPIClient instance
    """
    return TwitterAPIClient(use_mock=use_mock)
