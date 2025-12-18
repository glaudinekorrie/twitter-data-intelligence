#!/usr/bin/env python3
"""
Test script for Database Loader
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("?? Testing Database Loader")
print("=" * 50)

# Step 1: Test database initialization
print("\n1. Testing database initialization...")
try:
    from src.load.database_loader import DatabaseLoader, save_tweets_to_database
    print("? Successfully imported DatabaseLoader")
except ImportError as e:
    print(f"? Import error: {e}")
    print("\n?? Make sure:")
    print("   - You're in the project root directory")
    print("   - src/load/database_loader.py exists")
    print("   - The file has proper Python syntax")
    sys.exit(1)

# Step 2: Initialize database
print("\n2. Initializing database...")
try:
    # Create data directory if it doesn't exist
    os.makedirs('data/database', exist_ok=True)
    
    db_loader = DatabaseLoader(db_type='sqlite', db_path=':memory:')
    print("? Database initialized successfully")
    print(f"   Database path: {db_loader.db_path}")
except Exception as e:
    print(f"? Failed to initialize database: {e}")
    sys.exit(1)

# Step 3: Create test data
print("\n3. Creating test tweets...")
test_tweets = [
    {
        'tweet_id': 'test_001',
        'created_at': datetime.now().isoformat(),
        'content': 'Testing data engineering pipeline! #DataEngineering #Python',
        'user_id': 'user_001',
        'user_name': 'test_user',
        'user_display_name': 'Test User',
        'user_followers': 100,
        'user_following': 50,
        'retweet_count': 5,
        'favorite_count': 10,
        'reply_count': 2,
        'is_retweet': False,
        'language': 'en',
        'source': 'Web App',
        'sentiment_score': 0.8,
        'sentiment_category': 'positive',
        'brand_mentioned': 'Twitter',
        'collected_at': datetime.now().isoformat(),
        'hashtags': ['DataEngineering', 'Python'],
        'mentions': []
    },
    {
        'tweet_id': 'test_002',
        'created_at': datetime.now().isoformat(),
        'content': 'Building a Twitter analytics platform with Python and SQL. #BigData',
        'user_id': 'user_002',
        'user_name': 'data_scientist',
        'user_display_name': 'Data Scientist',
        'user_followers': 500,
        'user_following': 200,
        'retweet_count': 3,
        'favorite_count': 15,
        'reply_count': 1,
        'is_retweet': False,
        'language': 'en',
        'source': 'Twitter Web App',
        'sentiment_score': 0.6,
        'sentiment_category': 'positive',
        'brand_mentioned': 'Python',
        'collected_at': datetime.now().isoformat(),
        'hashtags': ['BigData'],
        'mentions': []
    }
]

print(f"? Created {len(test_tweets)} test tweets")

# Step 4: Save tweets to database
print("\n4. Saving tweets to database...")
try:
    saved_count = db_loader.save_tweets(test_tweets)
    print(f"? Saved {saved_count} tweets to database")
except Exception as e:
    print(f"? Error saving tweets: {e}")

# Step 5: Retrieve tweets
print("\n5. Retrieving tweets from database...")
try:
    df_tweets = db_loader.get_recent_tweets(limit=10)
    print(f"? Retrieved {len(df_tweets)} tweets from database")
    
    if not df_tweets.empty:
        print("\n?? Sample tweets from database:")
        print(df_tweets[['tweet_id', 'user_name', 'content', 'created_at']].to_string(index=False))
except Exception as e:
    print(f"? Error retrieving tweets: {e}")

# Step 6: Test brand mentions
print("\n6. Testing brand mentions...")
try:
    brand = 'Twitter'
    brand_tweets = db_loader.get_brand_mentions(brand, days=7)
    print(f"? Found {len(brand_tweets)} mentions of '{brand}'")
except Exception as e:
    print(f"? Error getting brand mentions: {e}")

# Step 7: Test daily stats
print("\n7. Testing daily statistics...")
try:
    stats = db_loader.get_daily_stats()
    if stats:
        print(f"? Daily stats retrieved:")
        print(f"   Total tweets: {stats.get('total_tweets', 0)}")
        print(f"   Unique users: {stats.get('unique_users', 0)}")
        
        if stats.get('top_brands'):
            print(f"   Top brand: {stats['top_brands'][0]['brand']} "
                  f"({stats['top_brands'][0]['mentions']} mentions)")
    else:
        print("??  No daily stats available")
except Exception as e:
    print(f"? Error getting daily stats: {e}")

# Step 8: Test utility function
print("\n8. Testing utility function...")
try:
    # Create one more test tweet
    new_tweet = [{
        'tweet_id': 'test_003',
        'created_at': datetime.now().isoformat(),
        'content': 'Utility function test #Testing',
        'user_id': 'user_003',
        'user_name': 'tester',
        'user_display_name': 'Tester',
        'user_followers': 50,
        'user_following': 100,
        'retweet_count': 1,
        'favorite_count': 3,
        'reply_count': 0,
        'is_retweet': False,
        'language': 'en',
        'source': 'Test',
        'sentiment_score': 0.5,
        'sentiment_category': 'neutral',
        'brand_mentioned': 'TestBrand',
        'collected_at': datetime.now().isoformat(),
        'hashtags': ['Testing'],
        'mentions': []
    }]
    
    saved = save_tweets_to_database(new_tweet)
    print(f"? Utility function saved {saved} tweet")
except Exception as e:
    print(f"? Error with utility function: {e}")

# Step 9: Database info
print("\n9. Database information:")
try:
    db_file = 'data/database/twitter.db'
    if os.path.exists(db_file):
        size_kb = os.path.getsize(db_file) / 1024
        print(f"   Database file: {db_file}")
        print(f"   File size: {size_kb:.1f} KB")
    
    # Get table counts
    cursor = db_loader.connection.cursor()
    
    tables = ['tweets', 'hashtags', 'mentions', 'brands']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   {table.capitalize()}: {count} rows")
        
except Exception as e:
    print(f"   Error getting database info: {e}")

# Clean up
db_loader.close()

print("\n" + "=" * 50)
print("?? Database loader test completed!")
print("\n? What was tested:")
print("   - Database initialization and table creation")
print("   - Saving tweets to SQLite")
print("   - Retrieving tweets with pagination")
print("   - Brand mention tracking")
print("   - Daily statistics generation")
print("   - Utility functions")

print("\n?? Next steps for your Twitter Data Engineering project:")
print("1. Connect this to your Twitter scraper (from previous chat)")
print("2. Add data quality checks")
print("3. Create ETL pipeline")
print("4. Add monitoring and logging")
print("5. Deploy to production")

print("\n?? To connect with your Twitter scraper:")
print("   tweets = twitter_scraper.get_tweets()")
print("   db_loader.save_tweets(tweets)")
