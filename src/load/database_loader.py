"""
Database loader for storing Twitter data in SQLite/PostgreSQL
Handles database operations with proper error handling and logging
"""
import sqlite3
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Load and manage Twitter data in databases"""
    
    def __init__(self, db_type: str = 'sqlite', db_path: str = None):
        """
        Initialize database loader
        
        Args:
            db_type: 'sqlite' or 'postgres'
            db_path: Path to SQLite database or connection string
        """
        self.db_type = db_type
        self.db_path = db_path or 'data/database/twitter.db'
        self.engine = None
        self.connection = None
        
        self._setup_database()
        self._create_tables()
    
    def _setup_database(self) -> None:
        """Set up database connection"""
        try:
            if self.db_type == 'sqlite':
                # Check if it's an in-memory database
                if self.db_path != ':memory:':
                    # Ensure directory exists for file-based databases
                    db_dir = os.path.dirname(self.db_path)
                    if db_dir:  # Only create directory if path has a directory component
                        os.makedirs(db_dir, exist_ok=True)
                
                # Create SQLite connection
                self.connection = sqlite3.connect(self.db_path)
                self.connection.row_factory = sqlite3.Row  # Return rows as dictionaries
                
                if self.db_path == ':memory:':
                    logger.info("Connected to in-memory SQLite database")
                else:
                    logger.info(f"SQLite database connected: {self.db_path}")
                
            elif self.db_type == 'postgres':
                try:
                    from sqlalchemy import create_engine
                    # PostgreSQL connection
                    if not self.db_path:
                        self.db_path = os.getenv('DATABASE_URL', 
                                               'postgresql://user:pass@localhost/twitter')
                    
                    self.engine = create_engine(self.db_path)
                    self.connection = self.engine.connect()
                    
                    logger.info(f"PostgreSQL database connected")
                    
                except ImportError:
                    logger.error("SQLAlchemy required for PostgreSQL. Install with: pip install sqlalchemy psycopg2")
                    raise ImportError("SQLAlchemy not available for PostgreSQL connection")
                
            else:
                logger.error(f"Database type {self.db_type} not supported")
                raise ValueError(f"Unsupported database type: {self.db_type}")
                
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    def _create_tables(self) -> None:
        """Create necessary tables if they don't exist"""
        try:
            # Tweets table
            tweets_table_sql = """
            CREATE TABLE IF NOT EXISTS tweets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP NOT NULL,
                content TEXT NOT NULL,
                user_id TEXT NOT NULL,
                user_name TEXT NOT NULL,
                user_display_name TEXT,
                user_followers INTEGER DEFAULT 0,
                user_following INTEGER DEFAULT 0,
                retweet_count INTEGER DEFAULT 0,
                favorite_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                is_retweet BOOLEAN DEFAULT FALSE,
                language TEXT DEFAULT 'en',
                source TEXT,
                sentiment_score REAL,
                sentiment_category TEXT,
                brand_mentioned TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                created_date DATE GENERATED ALWAYS AS (DATE(created_at)) STORED
            )
            """
            
            # Hashtags table (for normalization)
            hashtags_table_sql = """
            CREATE TABLE IF NOT EXISTS hashtags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT NOT NULL,
                hashtag TEXT NOT NULL,
                FOREIGN KEY (tweet_id) REFERENCES tweets (tweet_id),
                UNIQUE(tweet_id, hashtag)
            )
            """
            
            # Mentions table
            mentions_table_sql = """
            CREATE TABLE IF NOT EXISTS mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tweet_id TEXT NOT NULL,
                mentioned_user TEXT NOT NULL,
                FOREIGN KEY (tweet_id) REFERENCES tweets (tweet_id)
            )
            """
            
            # Brands table for tracking
            brands_table_sql = """
            CREATE TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                category TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Execute table creation
            cursor = self.connection.cursor()
            cursor.execute(tweets_table_sql)
            cursor.execute(hashtags_table_sql)
            cursor.execute(mentions_table_sql)
            cursor.execute(brands_table_sql)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_user ON tweets(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tweets_brand ON tweets(brand_mentioned)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hashtags_tag ON hashtags(hashtag)")
            
            self.connection.commit()
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def save_tweets(self, tweets_data: List[Dict[str, Any]]) -> int:
        """
        Save tweets to database
        
        Args:
            tweets_data: List of tweet dictionaries
            
        Returns:
            Number of tweets saved
        """
        if not tweets_data:
            logger.warning("No tweets to save")
            return 0
        
        saved_count = 0
        
        try:
            cursor = self.connection.cursor()
            
            for tweet in tweets_data:
                # Check if tweet already exists
                cursor.execute(
                    "SELECT tweet_id FROM tweets WHERE tweet_id = ?",
                    (tweet.get('tweet_id'),)
                )
                
                if cursor.fetchone():
                    logger.debug(f"Tweet {tweet.get('tweet_id')} already exists, skipping")
                    continue
                
                # Insert tweet
                tweet_insert_sql = """
                INSERT OR REPLACE INTO tweets (
                    tweet_id, created_at, content, user_id, user_name,
                    user_display_name, user_followers, user_following,
                    retweet_count, favorite_count, reply_count, is_retweet,
                    language, source, sentiment_score, sentiment_category,
                    brand_mentioned, collected_at, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor.execute(tweet_insert_sql, (
                    tweet.get('tweet_id'),
                    tweet.get('created_at'),
                    tweet.get('text') or tweet.get('content', ''),
                    tweet.get('user_id'),
                    tweet.get('user_name'),
                    tweet.get('user_display_name'),
                    tweet.get('user_followers', 0),
                    tweet.get('user_following', 0),
                    tweet.get('retweet_count', 0),
                    tweet.get('favorite_count', 0),
                    tweet.get('reply_count', 0),
                    1 if tweet.get('is_retweet') else 0,
                    tweet.get('language', 'en'),
                    tweet.get('source', ''),
                    tweet.get('sentiment_score'),
                    tweet.get('sentiment_category'),
                    tweet.get('brand_mentioned'),
                    tweet.get('collected_at', datetime.now().isoformat()),
                    datetime.now().isoformat()
                ))
                
                tweet_id = tweet.get('tweet_id')
                
                # Save hashtags
                hashtags = tweet.get('hashtags', [])
                for hashtag in hashtags:
                    if hashtag and isinstance(hashtag, str):  # Skip empty or non-string
                        cursor.execute(
                            "INSERT OR IGNORE INTO hashtags (tweet_id, hashtag) VALUES (?, ?)",
                            (tweet_id, hashtag.lower().strip())
                        )
                
                # Save mentions
                mentions = tweet.get('mentions', [])
                for mention in mentions:
                    if mention and isinstance(mention, str):  # Skip empty or non-string
                        cursor.execute(
                            "INSERT INTO mentions (tweet_id, mentioned_user) VALUES (?, ?)",
                            (tweet_id, mention.strip())
                        )
                
                saved_count += 1
            
            self.connection.commit()
            logger.info(f"Saved {saved_count} tweets to database")
            
            # Update brand mentions count
            self._update_brand_stats()
            
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving tweets: {e}")
            if self.connection:
                self.connection.rollback()
            return 0
    
    def save_tweets_with_sentiment(self, tweets_data: List[Dict[str, Any]], 
                              analyze_sentiment: bool = True) -> int:
        """
        Save tweets to database with optional sentiment analysis
        
        Args:
            tweets_data: List of tweet dictionaries
            analyze_sentiment: Whether to perform sentiment analysis
            
        Returns:
            Number of tweets saved
        """
        if not tweets_data:
            logger.warning("No tweets to save")
            return 0
        
        # Perform sentiment analysis if requested
        if analyze_sentiment:
            try:
                from src.transform.sentiment_analyzer import SentimentAnalyzer
                analyzer = SentimentAnalyzer()
                tweets_data = analyzer.analyze_tweets(tweets_data)
                logger.info("Sentiment analysis completed")
            except ImportError as e:
                logger.warning(f"Sentiment analyzer not available: {e}")
            except Exception as e:
                logger.error(f"Error in sentiment analysis: {e}")
        
        # Now save to database using existing method
        return self.save_tweets(tweets_data)

    def _update_brand_stats(self) -> None:
        """Update brand statistics"""
        try:
            cursor = self.connection.cursor()
            
            # Get unique brands from tweets
            cursor.execute("""
                SELECT DISTINCT brand_mentioned 
                FROM tweets 
                WHERE brand_mentioned IS NOT NULL 
                AND brand_mentioned != ''
            """)
            
            brands = [row[0] for row in cursor.fetchall()]
            
            # Insert new brands
            for brand in brands:
                if brand:  # Skip empty
                    cursor.execute(
                        "INSERT OR IGNORE INTO brands (name) VALUES (?)",
                        (brand.strip(),)
                    )
            
            self.connection.commit()
            
        except Exception as e:
            logger.error(f"Error updating brand stats: {e}")
    
    def get_recent_tweets(self, limit: int = 100) -> pd.DataFrame:
        """
        Get recent tweets from database
        
        Args:
            limit: Number of tweets to retrieve
            
        Returns:
            DataFrame of tweets
        """
        try:
            query = f"""
            SELECT * FROM tweets 
            ORDER BY created_at DESC 
            LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Retrieved {len(df)} tweets from database")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving tweets: {e}")
            return pd.DataFrame()
    
    def get_brand_mentions(self, brand: str, days: int = 7) -> pd.DataFrame:
        """
        Get mentions of a specific brand
        
        Args:
            brand: Brand name to search for
            days: Number of days to look back
            
        Returns:
            DataFrame of brand mentions
        """
        try:
            query = """
            SELECT * FROM tweets 
            WHERE brand_mentioned = ? 
            AND created_at >= datetime('now', ?)
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql_query(query, self.connection, 
                                 params=(brand, f'-{days} days'))
            logger.info(f"Found {len(df)} mentions of {brand} in last {days} days")
            return df
            
        except Exception as e:
            logger.error(f"Error getting brand mentions: {e}")
            return pd.DataFrame()
    
    def get_daily_stats(self, date: str = None) -> Dict[str, Any]:
        """
        Get daily statistics
        
        Args:
            date: Date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            Dictionary of daily statistics
        """
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            cursor = self.connection.cursor()
            
            # Get total tweets for the day
            cursor.execute("""
                SELECT COUNT(*) as total_tweets,
                       COUNT(DISTINCT user_id) as unique_users,
                       AVG(retweet_count) as avg_retweets,
                       AVG(favorite_count) as avg_favorites
                FROM tweets 
                WHERE DATE(created_at) = ?
            """, (date,))
            
            row = cursor.fetchone()
            stats = {
                'total_tweets': row[0] if row else 0,
                'unique_users': row[1] if row else 0,
                'avg_retweets': float(row[2]) if row and row[2] else 0.0,
                'avg_favorites': float(row[3]) if row and row[3] else 0.0
            }
            
            # Get top brands
            cursor.execute("""
                SELECT brand_mentioned, COUNT(*) as mention_count
                FROM tweets 
                WHERE DATE(created_at) = ? 
                AND brand_mentioned IS NOT NULL
                AND brand_mentioned != ''
                GROUP BY brand_mentioned
                ORDER BY mention_count DESC
                LIMIT 5
            """, (date,))
            
            stats['top_brands'] = [
                {'brand': row[0], 'mentions': row[1]} 
                for row in cursor.fetchall()
            ]
            
            # Get top hashtags
            cursor.execute("""
                SELECT h.hashtag, COUNT(*) as usage_count
                FROM hashtags h
                JOIN tweets t ON h.tweet_id = t.tweet_id
                WHERE DATE(t.created_at) = ?
                GROUP BY h.hashtag
                ORDER BY usage_count DESC
                LIMIT 10
            """, (date,))
            
            stats['top_hashtags'] = [
                {'hashtag': row[0], 'count': row[1]} 
                for row in cursor.fetchall()
            ]
            
            logger.info(f"Retrieved daily stats for {date}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting daily stats: {e}")
            return {}
    
    def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Utility functions
def save_tweets_to_database(tweets: List[Dict[str, Any]], 
                          db_type: str = 'sqlite',
                          db_path: str = None) -> int:
    """
    Utility function to save tweets to database
    
    Args:
        tweets: List of tweet dictionaries
        db_type: 'sqlite' or 'postgres'
        db_path: Database path or connection string
        
    Returns:
        Number of tweets saved
    """
    with DatabaseLoader(db_type=db_type, db_path=db_path) as loader:
        return loader.save_tweets(tweets)


def load_tweets_from_database(limit: int = 100,
                            db_type: str = 'sqlite',
                            db_path: str = None) -> pd.DataFrame:
    """
    Utility function to load tweets from database
    
    Args:
        limit: Number of tweets to load
        db_type: 'sqlite' or 'postgres'
        db_path: Database path or connection string
        
    Returns:
        DataFrame of tweets
    """
    with DatabaseLoader(db_type=db_type, db_path=db_path) as loader:
        return loader.get_recent_tweets(limit)